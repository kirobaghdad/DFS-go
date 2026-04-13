package main

import (
	"context"
	"encoding/binary"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	pb "dfs-go/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

const (
	commandUpload       byte = 0x01
	commandDownload     byte = 0x02
	commandRangeRequest byte = 0x03

	heartbeatEvery          = 1 * time.Second
	heartbeatRPCTimeout     = 10 * time.Second
	reportFilesRPCTimeout   = 20 * time.Second
	notifyUploadRPCTimeout  = 15 * time.Second
	transferProgressTimeout = 1 * time.Second
	transferProgressEvery   = 750 * time.Millisecond
	transferProgressStep    = 4 * 1024 * 1024
	supportedMediaSuffix    = ".mp4"
	transferBufferSize      = 1024 * 1024
	socketBufferSize        = 4 * 1024 * 1024
)

type DataKeeper struct {
	pb.UnimplementedDataKeeperServer

	id         string
	ip         string
	tcpPort    int32
	masterAddr string
	master     pb.MasterTrackerClient
	baseDir    string

	replicationMu   sync.Mutex
	activeTransfers map[string]transferHandle
	transfersByFile map[string]map[string]struct{}
}

type transferHandle struct {
	fileName string
	cancel   context.CancelFunc
}

func NewDataKeeper(id, ip string, tcpPort int32, masterAddr string, master pb.MasterTrackerClient) *DataKeeper {
	baseDir := fmt.Sprintf("data_%s", id)
	if err := os.MkdirAll(baseDir, 0o755); err != nil {
		log.Fatalf("failed to create data directory %s: %v", baseDir, err)
	}

	return &DataKeeper{
		id:              id,
		ip:              ip,
		tcpPort:         tcpPort,
		masterAddr:      masterAddr,
		master:          master,
		baseDir:         baseDir,
		activeTransfers: make(map[string]transferHandle),
		transfersByFile: make(map[string]map[string]struct{}),
	}
}

func (dk *DataKeeper) SendHeartbeat() {
	dk.SyncExistingFiles()

	ticker := time.NewTicker(heartbeatEvery)
	defer ticker.Stop()

	for range ticker.C {
		ctx, cancel := context.WithTimeout(context.Background(), heartbeatRPCTimeout)
		_, err := dk.master.Heartbeat(ctx, &pb.HeartbeatRequest{
			NodeId: dk.id,
			Ip:     dk.ip,
			Port:   dk.tcpPort,
		}, grpc.WaitForReady(true))
		cancel()
		if err != nil {
			dk.logMasterRPCError("heartbeat", err)
		}
	}
}

func (dk *DataKeeper) SyncExistingFiles() {
	entries, err := os.ReadDir(dk.baseDir)
	if err != nil {
		log.Printf("Failed to scan %s: %v", dk.baseDir, err)
		return
	}

	fileInfos := make([]*pb.FileInfo, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() || !strings.EqualFold(filepath.Ext(entry.Name()), supportedMediaSuffix) {
			continue
		}

		filePath := filepath.Join(dk.baseDir, entry.Name())
		info, err := os.Stat(filePath)
		if err != nil {
			log.Printf("Skipping %s during startup sync: %v", filePath, err)
			continue
		}

		fileInfos = append(fileInfos, &pb.FileInfo{
			FileName: entry.Name(),
			FileSize: info.Size(),
			FilePath: filePath,
		})
	}

	ctx, cancel := context.WithTimeout(context.Background(), reportFilesRPCTimeout)
	defer cancel()

	if _, err := dk.master.ReportFiles(ctx, &pb.ReportFilesRequest{
		NodeId: dk.id,
		Files:  fileInfos,
	}, grpc.WaitForReady(true)); err != nil {
		dk.logMasterRPCError("startup file sync", err)
		return
	}

	if len(fileInfos) > 0 {
		log.Printf("Reported %d local file(s) to master", len(fileInfos))
	}
}

func (dk *DataKeeper) StartTCPServer() {
	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", dk.tcpPort))
	if err != nil {
		log.Fatalf("failed to listen on TCP %d: %v", dk.tcpPort, err)
	}
	log.Printf("Data Keeper TCP server listening on :%d", dk.tcpPort)

	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Printf("TCP accept failed: %v", err)
			continue
		}
		configureTCPConn(conn)
		go dk.handleTCPConnection(conn)
	}
}

func (dk *DataKeeper) handleTCPConnection(conn net.Conn) {
	defer conn.Close()

	command := make([]byte, 1)
	if _, err := io.ReadFull(conn, command); err != nil {
		log.Printf("Failed to read command byte: %v", err)
		return
	}

	switch command[0] {
	case commandUpload:
		uploadID, err := readSizedString(conn)
		if err != nil {
			log.Printf("Failed to read upload id: %v", err)
			return
		}
		fileName, err := readSizedString(conn)
		if err != nil {
			log.Printf("Failed to read upload file name: %v", err)
			return
		}
		expectedSize, err := readInt64(conn)
		if err != nil {
			log.Printf("Failed to read expected upload size: %v", err)
			return
		}
		if err := dk.receiveUpload(uploadID, fileName, expectedSize, conn); err != nil {
			log.Printf("Upload receive failed: %v", err)
		}
	case commandDownload:
		fileName, err := readSizedString(conn)
		if err != nil {
			log.Printf("Failed to read download file name: %v", err)
			return
		}
		if err := dk.streamWholeFile(fileName, conn); err != nil {
			log.Printf("Download serve failed: %v", err)
		}
	case commandRangeRequest:
		fileName, err := readSizedString(conn)
		if err != nil {
			log.Printf("Failed to read range file name: %v", err)
			return
		}
		start, err := readInt64(conn)
		if err != nil {
			log.Printf("Failed to read range start: %v", err)
			return
		}
		length, err := readInt64(conn)
		if err != nil {
			log.Printf("Failed to read range length: %v", err)
			return
		}
		if err := dk.streamFileRange(fileName, start, length, conn); err != nil {
			log.Printf("Range serve failed: %v", err)
		}
	default:
		log.Printf("Unknown TCP command %d", command[0])
	}
}

func (dk *DataKeeper) receiveUpload(uploadID, rawFileName string, expectedSize int64, source io.Reader) error {
	fileName, filePath, err := dk.resolveLocalPath(rawFileName)
	if err != nil {
		return err
	}

	if expectedSize < 0 {
		return fmt.Errorf("invalid expected size %d", expectedSize)
	}

	transferCtx, cancel := context.WithCancel(context.Background())
	transferKey := incomingTransferKey(fileName, uploadID)
	if !dk.startTransfer(transferKey, fileName, cancel) {
		cancel()
		return fmt.Errorf("another transfer for %s is already using key %s", fileName, transferKey)
	}
	defer func() {
		cancel()
		dk.finishTransfer(transferKey)
	}()
	closeTransferOnCancel(transferCtx, source)

	tempPath := filePath + ".part"
	file, err := os.Create(tempPath)
	if err != nil {
		return fmt.Errorf("failed to create %s: %w", tempPath, err)
	}

	size, err := copyExactWithBuffer(transferCtx, file, source, expectedSize, nil)
	closeErr := file.Close()
	if err != nil {
		_ = os.Remove(tempPath)
		return fmt.Errorf("failed to copy file payload: received %d of %d byte(s): %w", size, expectedSize, err)
	}
	if closeErr != nil {
		_ = os.Remove(tempPath)
		return fmt.Errorf("failed to finalize temp file: %w", closeErr)
	}
	if size != expectedSize {
		_ = os.Remove(tempPath)
		return fmt.Errorf("upload incomplete: received %d of %d byte(s)", size, expectedSize)
	}
	if err := os.Rename(tempPath, filePath); err != nil {
		_ = os.Remove(tempPath)
		return fmt.Errorf("failed to move completed file into place: %w", err)
	}

	log.Printf("Stored %s locally at %s (%d byte(s))", fileName, filePath, size)

	ctx, cancel := context.WithTimeout(context.Background(), notifyUploadRPCTimeout)
	defer cancel()

	_, err = dk.master.NotifyUpload(ctx, &pb.NotifyUploadRequest{
		UploadId: uploadID,
		FileName: fileName,
		NodeId:   dk.id,
		FilePath: filePath,
		FileSize: size,
	}, grpc.WaitForReady(true))
	if err != nil {
		return fmt.Errorf("failed to notify master: %w", err)
	}

	return nil
}

func (dk *DataKeeper) streamWholeFile(rawFileName string, destination io.Writer) error {
	_, filePath, err := dk.resolveLocalPath(rawFileName)
	if err != nil {
		return err
	}

	file, err := os.Open(filePath)
	if err != nil {
		return fmt.Errorf("failed to open %s: %w", filePath, err)
	}
	defer file.Close()

	if _, err := io.CopyBuffer(destination, file, make([]byte, transferBufferSize)); err != nil {
		return fmt.Errorf("failed to send file: %w", err)
	}

	log.Printf("Served full download for %s", filepath.Base(filePath))
	return nil
}

func (dk *DataKeeper) streamFileRange(rawFileName string, start, length int64, destination io.Writer) error {
	if start < 0 || length < 0 {
		return errors.New("range values must be non-negative")
	}

	_, filePath, err := dk.resolveLocalPath(rawFileName)
	if err != nil {
		return err
	}

	file, err := os.Open(filePath)
	if err != nil {
		return fmt.Errorf("failed to open %s: %w", filePath, err)
	}
	defer file.Close()

	if _, err := file.Seek(start, io.SeekStart); err != nil {
		return fmt.Errorf("failed to seek file: %w", err)
	}

	if length == 0 {
		return nil
	}

	if _, err := io.CopyBuffer(destination, io.LimitReader(file, length), make([]byte, transferBufferSize)); err != nil {
		return fmt.Errorf("failed to send file range: %w", err)
	}

	log.Printf("Served %s range [%d:%d]", filepath.Base(filePath), start, start+length)
	return nil
}

func (dk *DataKeeper) DeleteFile(ctx context.Context, req *pb.DeleteFileRequest) (*pb.DeleteFileResponse, error) {
	fileName, filePath, err := dk.resolveLocalPath(req.GetFileName())
	if err != nil {
		return &pb.DeleteFileResponse{Success: false}, err
	}

	canceled := dk.cancelTransfersForFile(fileName)
	deleteErr := removeIfExists(filePath)
	tempErr := removeIfExists(filePath + ".part")
	if deleteErr != nil {
		return &pb.DeleteFileResponse{Success: false}, deleteErr
	}
	if tempErr != nil {
		return &pb.DeleteFileResponse{Success: false}, tempErr
	}

	log.Printf("Deleted local file %s and canceled %d in-flight transfer(s)", filepath.Base(filePath), canceled)
	return &pb.DeleteFileResponse{Success: true}, nil
}

func (dk *DataKeeper) Wipe(ctx context.Context, req *pb.WipeRequest) (*pb.WipeResponse, error) {
	canceled := dk.cancelAllTransfers()

	entries, err := os.ReadDir(dk.baseDir)
	if err != nil {
		return &pb.WipeResponse{Success: false}, err
	}

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		if err := os.Remove(filepath.Join(dk.baseDir, entry.Name())); err != nil {
			log.Printf("Failed to remove %s during wipe: %v", entry.Name(), err)
		}
	}

	log.Printf("Wiped local storage in %s and canceled %d in-flight transfer(s)", dk.baseDir, canceled)
	return &pb.WipeResponse{Success: true}, nil
}

func (dk *DataKeeper) Replicate(ctx context.Context, req *pb.ReplicateRequest) (*pb.ReplicateResponse, error) {
	fileName, filePath, err := dk.resolveLocalPath(req.GetFileName())
	if err != nil {
		return nil, err
	}

	file, err := os.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to open %s for replication: %w", filePath, err)
	}
	fileInfo, err := file.Stat()
	closeErr := file.Close()
	if err != nil {
		return nil, fmt.Errorf("failed to stat %s for replication: %w", filePath, err)
	}
	if closeErr != nil {
		return nil, fmt.Errorf("failed to close %s after stat: %w", filePath, closeErr)
	}

	transferID := strings.TrimSpace(req.GetTransferId())
	if transferID == "" {
		return nil, errors.New("transfer_id is required")
	}

	transferCtx, cancel := context.WithCancel(context.Background())
	jobKey := replicationJobKey(fileName, req.GetDestinationNodeId(), transferID)
	if !dk.startTransfer(jobKey, fileName, cancel) {
		cancel()
		log.Printf("Replication for %s to %s is already running locally", fileName, req.GetDestinationNodeId())
		return &pb.ReplicateResponse{Success: true}, nil
	}

	go dk.runReplication(transferCtx, cancel, jobKey, req, filePath, fileInfo.Size())
	return &pb.ReplicateResponse{Success: true}, nil
}

func (dk *DataKeeper) runReplication(transferCtx context.Context, cancel context.CancelFunc, jobKey string, req *pb.ReplicateRequest, filePath string, fileSize int64) {
	defer cancel()
	defer dk.finishTransfer(jobKey)

	file, err := os.Open(filePath)
	if err != nil {
		dk.reportTransferProgress(req, 0, fileSize, pb.TransferStatus_TRANSFER_STATUS_FAILED, fmt.Sprintf("failed to open source file: %v", err), true)
		return
	}
	defer file.Close()

	dk.reportTransferProgress(req, 0, fileSize, pb.TransferStatus_TRANSFER_STATUS_QUEUED, "source node accepted the replication command", false)

	conn, err := net.Dial("tcp", net.JoinHostPort(req.GetDestinationIp(), fmt.Sprintf("%d", req.GetDestinationPort())))
	if err != nil {
		dk.reportTransferProgress(req, 0, fileSize, pb.TransferStatus_TRANSFER_STATUS_FAILED, fmt.Sprintf("failed to connect to destination keeper: %v", err), true)
		return
	}
	defer conn.Close()
	configureTCPConn(conn)
	closeTransferOnCancel(transferCtx, conn)

	if _, err := conn.Write([]byte{commandUpload}); err != nil {
		dk.reportTransferProgress(req, 0, fileSize, pb.TransferStatus_TRANSFER_STATUS_FAILED, fmt.Sprintf("failed to send replication command: %v", err), true)
		return
	}
	if err := writeSizedString(conn, ""); err != nil {
		dk.reportTransferProgress(req, 0, fileSize, pb.TransferStatus_TRANSFER_STATUS_FAILED, fmt.Sprintf("failed to send replication upload id: %v", err), true)
		return
	}
	if err := writeSizedString(conn, req.GetFileName()); err != nil {
		dk.reportTransferProgress(req, 0, fileSize, pb.TransferStatus_TRANSFER_STATUS_FAILED, fmt.Sprintf("failed to send replication file name: %v", err), true)
		return
	}
	if err := binary.Write(conn, binary.BigEndian, fileSize); err != nil {
		dk.reportTransferProgress(req, 0, fileSize, pb.TransferStatus_TRANSFER_STATUS_FAILED, fmt.Sprintf("failed to send replication file size: %v", err), true)
		return
	}

	reporter := newTransferReporter(func(bytesTransferred int64, status pb.TransferStatus, message string, logFailures bool) {
		dk.reportTransferProgress(req, bytesTransferred, fileSize, status, message, logFailures)
	})
	defer reporter.close()
	reporter.report(0, pb.TransferStatus_TRANSFER_STATUS_RUNNING, "streaming data to the destination keeper", false)

	written, err := copyExactWithBuffer(transferCtx, conn, file, fileSize, func(bytesTransferred int64) {
		reporter.maybeReport(bytesTransferred, pb.TransferStatus_TRANSFER_STATUS_RUNNING, "streaming data to the destination keeper")
	})
	if err != nil {
		reporter.report(written, pb.TransferStatus_TRANSFER_STATUS_FAILED, fmt.Sprintf("replication stopped after %d of %d byte(s): %v", written, fileSize, err), true)
		return
	}

	reporter.report(fileSize, pb.TransferStatus_TRANSFER_STATUS_AWAITING_CONFIRMATION, "stream finished; waiting for destination confirmation", false)
	log.Printf("Replication stream for %s to %s:%d finished", req.GetFileName(), req.GetDestinationIp(), req.GetDestinationPort())
}

func (dk *DataKeeper) reportTransferProgress(req *pb.ReplicateRequest, bytesTransferred, totalBytes int64, status pb.TransferStatus, message string, logFailures bool) {
	ctx, cancel := context.WithTimeout(context.Background(), transferProgressTimeout)
	defer cancel()

	_, err := dk.master.ReportTransferProgress(ctx, &pb.TransferProgressRequest{
		TransferId:        req.GetTransferId(),
		FileName:          req.GetFileName(),
		SourceNodeId:      dk.id,
		DestinationNodeId: req.GetDestinationNodeId(),
		BytesTransferred:  bytesTransferred,
		TotalBytes:        totalBytes,
		Status:            status,
		Message:           message,
	})
	if err != nil && logFailures {
		log.Printf("Failed to report transfer progress for %s to master: %v", req.GetTransferId(), err)
	}
}

func (dk *DataKeeper) startTransfer(jobKey, fileName string, cancel context.CancelFunc) bool {
	dk.replicationMu.Lock()
	defer dk.replicationMu.Unlock()

	if _, exists := dk.activeTransfers[jobKey]; exists {
		return false
	}

	dk.activeTransfers[jobKey] = transferHandle{
		fileName: fileName,
		cancel:   cancel,
	}
	if dk.transfersByFile[fileName] == nil {
		dk.transfersByFile[fileName] = make(map[string]struct{})
	}
	dk.transfersByFile[fileName][jobKey] = struct{}{}
	return true
}

func (dk *DataKeeper) finishTransfer(jobKey string) {
	dk.replicationMu.Lock()
	handle, exists := dk.activeTransfers[jobKey]
	if exists {
		delete(dk.activeTransfers, jobKey)
		if fileTransfers := dk.transfersByFile[handle.fileName]; fileTransfers != nil {
			delete(fileTransfers, jobKey)
			if len(fileTransfers) == 0 {
				delete(dk.transfersByFile, handle.fileName)
			}
		}
	}
	dk.replicationMu.Unlock()
}

func (dk *DataKeeper) cancelTransfersForFile(fileName string) int {
	dk.replicationMu.Lock()
	handles := dk.collectTransfersForFileLocked(fileName)
	dk.replicationMu.Unlock()

	for _, handle := range handles {
		handle.cancel()
	}
	return len(handles)
}

func (dk *DataKeeper) cancelAllTransfers() int {
	dk.replicationMu.Lock()
	handles := make([]transferHandle, 0, len(dk.activeTransfers))
	for _, handle := range dk.activeTransfers {
		handles = append(handles, handle)
	}
	dk.replicationMu.Unlock()

	for _, handle := range handles {
		handle.cancel()
	}
	return len(handles)
}

func (dk *DataKeeper) collectTransfersForFileLocked(fileName string) []transferHandle {
	transferIDs := dk.transfersByFile[fileName]
	handles := make([]transferHandle, 0, len(transferIDs))
	for transferID := range transferIDs {
		handle, exists := dk.activeTransfers[transferID]
		if exists {
			handles = append(handles, handle)
		}
	}
	return handles
}

func (dk *DataKeeper) StartRPCServer(port int32) {
	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		log.Fatalf("failed to listen for gRPC on %d: %v", port, err)
	}

	server := grpc.NewServer()
	pb.RegisterDataKeeperServer(server, dk)
	log.Printf("Data Keeper gRPC server listening on :%d", port)
	if err := server.Serve(listener); err != nil {
		log.Fatalf("failed to serve gRPC: %v", err)
	}
}

func (dk *DataKeeper) resolveLocalPath(rawFileName string) (string, string, error) {
	fileName := filepath.Base(strings.TrimSpace(rawFileName))
	if fileName == "" || fileName == "." {
		return "", "", errors.New("invalid file name")
	}
	return fileName, filepath.Join(dk.baseDir, fileName), nil
}

func readSizedString(reader io.Reader) (string, error) {
	length, err := readUint32(reader)
	if err != nil {
		return "", err
	}

	buffer := make([]byte, length)
	if _, err := io.ReadFull(reader, buffer); err != nil {
		return "", err
	}

	return string(buffer), nil
}

func writeSizedString(writer io.Writer, value string) error {
	if err := binary.Write(writer, binary.BigEndian, uint32(len(value))); err != nil {
		return err
	}
	_, err := writer.Write([]byte(value))
	return err
}

func readUint32(reader io.Reader) (uint32, error) {
	var value uint32
	err := binary.Read(reader, binary.BigEndian, &value)
	return value, err
}

func readInt64(reader io.Reader) (int64, error) {
	var value int64
	err := binary.Read(reader, binary.BigEndian, &value)
	return value, err
}

func configureTCPConn(conn net.Conn) {
	tcpConn, ok := conn.(*net.TCPConn)
	if !ok {
		return
	}
	_ = tcpConn.SetReadBuffer(socketBufferSize)
	_ = tcpConn.SetWriteBuffer(socketBufferSize)
}

func copyExactWithBuffer(ctx context.Context, destination io.Writer, source io.Reader, totalBytes int64, onProgress func(int64)) (int64, error) {
	if totalBytes < 0 {
		return 0, fmt.Errorf("invalid byte count %d", totalBytes)
	}
	if totalBytes == 0 {
		if onProgress != nil {
			onProgress(0)
		}
		return 0, nil
	}

	buffer := make([]byte, transferBufferSize)
	limitedSource := io.LimitReader(source, totalBytes)
	var written int64

	for written < totalBytes {
		if err := ctx.Err(); err != nil {
			return written, err
		}

		readCount, readErr := limitedSource.Read(buffer)
		if readCount > 0 {
			writeCount, writeErr := destination.Write(buffer[:readCount])
			written += int64(writeCount)
			if onProgress != nil {
				onProgress(written)
			}
			if err := ctx.Err(); err != nil {
				return written, err
			}
			if writeErr != nil {
				return written, writeErr
			}
			if writeCount != readCount {
				return written, io.ErrShortWrite
			}
		}

		if readErr != nil {
			if errors.Is(readErr, io.EOF) {
				break
			}
			return written, readErr
		}
	}

	if written != totalBytes {
		return written, io.ErrUnexpectedEOF
	}

	return written, nil
}

func closeTransferOnCancel(ctx context.Context, endpoint any) {
	closer, ok := endpoint.(interface{ Close() error })
	if !ok {
		return
	}

	go func() {
		<-ctx.Done()
		_ = closer.Close()
	}()
}

func removeIfExists(path string) error {
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}

type transferReporter struct {
	callback         func(int64, pb.TransferStatus, string, bool)
	updates          chan transferUpdate
	done             chan struct{}
	lastReportedAt   time.Time
	lastReportedByte int64
}

type transferUpdate struct {
	bytesTransferred int64
	status           pb.TransferStatus
	message          string
	logFailures      bool
}

func newTransferReporter(callback func(int64, pb.TransferStatus, string, bool)) *transferReporter {
	reporter := &transferReporter{
		callback: callback,
		updates:  make(chan transferUpdate, 1),
		done:     make(chan struct{}),
	}

	go func() {
		defer close(reporter.done)
		for update := range reporter.updates {
			if reporter.callback == nil {
				continue
			}
			reporter.callback(update.bytesTransferred, update.status, update.message, update.logFailures)
		}
	}()

	return reporter
}

func (tr *transferReporter) maybeReport(bytesTransferred int64, status pb.TransferStatus, message string) {
	if tr.callback == nil {
		return
	}

	if bytesTransferred-tr.lastReportedByte < transferProgressStep && time.Since(tr.lastReportedAt) < transferProgressEvery {
		return
	}

	tr.report(bytesTransferred, status, message, false)
}

func (tr *transferReporter) report(bytesTransferred int64, status pb.TransferStatus, message string, logFailures bool) {
	if tr.callback == nil {
		return
	}

	tr.lastReportedByte = bytesTransferred
	tr.lastReportedAt = time.Now()
	tr.enqueue(transferUpdate{
		bytesTransferred: bytesTransferred,
		status:           status,
		message:          message,
		logFailures:      logFailures,
	})
}

func (tr *transferReporter) enqueue(update transferUpdate) {
	select {
	case tr.updates <- update:
		return
	default:
	}

	select {
	case <-tr.updates:
	default:
	}

	tr.updates <- update
}

func (tr *transferReporter) close() {
	if tr.callback == nil {
		return
	}
	close(tr.updates)
	<-tr.done
}

func replicationJobKey(fileName, destinationNodeID, transferID string) string {
	return strings.Join([]string{fileName, destinationNodeID, transferID}, "\x00")
}

func incomingTransferKey(fileName, uploadID string) string {
	if strings.TrimSpace(uploadID) == "" {
		return "incoming:" + fileName
	}
	return "incoming:" + fileName + ":" + uploadID
}

func detectAdvertiseIPv4(masterAddr string) (string, error) {
	host, _, err := net.SplitHostPort(masterAddr)
	if err == nil && host != "" && host != "localhost" {
		conn, dialErr := net.Dial("udp", net.JoinHostPort(host, "53"))
		if dialErr == nil {
			defer conn.Close()
			if localAddr, ok := conn.LocalAddr().(*net.UDPAddr); ok {
				if ip := localAddr.IP.To4(); ip != nil && !ip.IsLoopback() {
					return ip.String(), nil
				}
			}
		}
	}

	ifaces, err := net.Interfaces()
	if err != nil {
		return "", err
	}

	for _, iface := range ifaces {
		if iface.Flags&net.FlagUp == 0 || iface.Flags&net.FlagLoopback != 0 {
			continue
		}

		addrs, err := iface.Addrs()
		if err != nil {
			continue
		}

		for _, addr := range addrs {
			var ip net.IP
			switch value := addr.(type) {
			case *net.IPNet:
				ip = value.IP
			case *net.IPAddr:
				ip = value.IP
			}

			ip = ip.To4()
			if ip == nil || ip.IsLoopback() {
				continue
			}

			return ip.String(), nil
		}
	}

	if host == "" || host == "localhost" || host == "127.0.0.1" || host == "::1" {
		return "127.0.0.1", nil
	}

	return "", fmt.Errorf("no non-loopback IPv4 found")
}

func main() {
	id := flag.String("id", "node1", "Node ID")
	tcpPort := flag.Int("port", 17000, "TCP port for file transfer")
	masterAddr := flag.String("master", "localhost:56051", "Master Tracker address")
	advertiseIP := flag.String("advertise-ip", "", "Node IP/hostname advertised to master (auto-detected if empty)")
	flag.Parse()

	conn, err := grpc.NewClient(*masterAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("did not connect to master: %v", err)
	}
	defer conn.Close()

	masterClient := pb.NewMasterTrackerClient(conn)
	ip := strings.TrimSpace(*advertiseIP)
	if ip == "" {
		detectedIP, err := detectAdvertiseIPv4(*masterAddr)
		if err != nil {
			log.Fatalf("failed to auto-detect node IP, set -advertise-ip explicitly: %v", err)
		}
		ip = detectedIP
	}

	log.Printf("Node %s advertising %s:%d to master %s", *id, ip, *tcpPort, *masterAddr)
	dataKeeper := NewDataKeeper(*id, ip, int32(*tcpPort), *masterAddr, masterClient)

	go dataKeeper.SendHeartbeat()
	go dataKeeper.StartTCPServer()

	dataKeeper.StartRPCServer(int32(*tcpPort) + 1000)
}

func (dk *DataKeeper) logMasterRPCError(operation string, err error) {
	st, ok := status.FromError(err)
	if !ok {
		log.Printf("%s to master %s failed: %v", strings.Title(operation), dk.masterAddr, err)
		return
	}

	switch st.Code() {
	case codes.DeadlineExceeded:
		log.Printf("%s to master %s timed out. Check that the master is running, the IP/port is correct, and the firewall allows gRPC on that port.", strings.Title(operation), dk.masterAddr)
	case codes.Unavailable:
		log.Printf("%s to master %s failed because the master is unavailable. Check startup order, IP address, and network reachability.", strings.Title(operation), dk.masterAddr)
	default:
		log.Printf("%s to master %s failed: %s", strings.Title(operation), dk.masterAddr, st.Message())
	}
}
