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

	heartbeatEvery         = 1 * time.Second
	heartbeatRPCTimeout    = 10 * time.Second
	reportFilesRPCTimeout  = 20 * time.Second
	notifyUploadRPCTimeout = 15 * time.Second
	supportedMediaSuffix   = ".mp4"
	transferBufferSize     = 1024 * 1024
	socketBufferSize       = 4 * 1024 * 1024
)

type DataKeeper struct {
	pb.UnimplementedDataKeeperServer

	id         string
	ip         string
	tcpPort    int32
	masterAddr string
	master     pb.MasterTrackerClient
	baseDir    string
}

func NewDataKeeper(id, ip string, tcpPort int32, masterAddr string, master pb.MasterTrackerClient) *DataKeeper {
	baseDir := fmt.Sprintf("data_%s", id)
	if err := os.MkdirAll(baseDir, 0o755); err != nil {
		log.Fatalf("failed to create data directory %s: %v", baseDir, err)
	}

	return &DataKeeper{
		id:         id,
		ip:         ip,
		tcpPort:    tcpPort,
		masterAddr: masterAddr,
		master:     master,
		baseDir:    baseDir,
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

	tempPath := filePath + ".part"
	file, err := os.Create(tempPath)
	if err != nil {
		return fmt.Errorf("failed to create %s: %w", tempPath, err)
	}

	size, err := io.CopyN(file, source, expectedSize)
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

	if _, err := io.Copy(destination, file); err != nil {
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

	if _, err := io.CopyN(destination, file, length); err != nil {
		return fmt.Errorf("failed to send file range: %w", err)
	}

	log.Printf("Served %s range [%d:%d]", filepath.Base(filePath), start, start+length)
	return nil
}

func (dk *DataKeeper) DeleteFile(ctx context.Context, req *pb.DeleteFileRequest) (*pb.DeleteFileResponse, error) {
	_, filePath, err := dk.resolveLocalPath(req.GetFileName())
	if err != nil {
		return &pb.DeleteFileResponse{Success: false}, err
	}

	if err := os.Remove(filePath); err != nil {
		if os.IsNotExist(err) {
			return &pb.DeleteFileResponse{Success: true}, nil
		}
		return &pb.DeleteFileResponse{Success: false}, err
	}

	log.Printf("Deleted local file %s", filepath.Base(filePath))
	return &pb.DeleteFileResponse{Success: true}, nil
}

func (dk *DataKeeper) Wipe(ctx context.Context, req *pb.WipeRequest) (*pb.WipeResponse, error) {
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

	log.Printf("Wiped local storage in %s", dk.baseDir)
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
	defer file.Close()
	fileInfo, err := file.Stat()
	if err != nil {
		return nil, fmt.Errorf("failed to stat %s for replication: %w", filePath, err)
	}

	conn, err := net.Dial("tcp", net.JoinHostPort(req.GetDestinationIp(), fmt.Sprintf("%d", req.GetDestinationPort())))
	if err != nil {
		return nil, fmt.Errorf("failed to connect to destination keeper: %w", err)
	}
	defer conn.Close()
	configureTCPConn(conn)

	if _, err := conn.Write([]byte{commandUpload}); err != nil {
		return nil, fmt.Errorf("failed to send replication command: %w", err)
	}
	if err := writeSizedString(conn, ""); err != nil {
		return nil, fmt.Errorf("failed to send replication upload id: %w", err)
	}
	if err := writeSizedString(conn, fileName); err != nil {
		return nil, fmt.Errorf("failed to send replication file name: %w", err)
	}
	if err := binary.Write(conn, binary.BigEndian, fileInfo.Size()); err != nil {
		return nil, fmt.Errorf("failed to send replication file size: %w", err)
	}
	if _, err := io.CopyBuffer(conn, file, make([]byte, transferBufferSize)); err != nil {
		return nil, fmt.Errorf("failed to stream replicated file: %w", err)
	}

	log.Printf("Copied %s to %s:%d", fileName, req.GetDestinationIp(), req.GetDestinationPort())
	return &pb.ReplicateResponse{Success: true}, nil
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
