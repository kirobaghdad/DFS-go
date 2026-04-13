package main

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
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
	clientMasterRPCTimeout      = 5 * time.Second
	uploadConfirmationWait      = 30 * time.Second
	dataKeeperDialTimeout       = 5 * time.Second
	uploadCommand          byte = 0x01
	downloadCommand        byte = 0x02
	rangeCommand           byte = 0x03
	allowedUploadSuffix         = ".mp4"
)

type ClientApp struct {
	master pb.MasterTrackerClient
}

type downloadRange struct {
	start  int64
	length int64
}

type chunkResult struct {
	index int
	data  []byte
	err   error
}

func main() {
	masterAddr := flag.String("master", "localhost:56051", "Master Tracker address")
	httpPort := flag.String("port", "18081", "HTTP port for Client GUI")
	flag.Parse()

	conn, err := grpc.NewClient(*masterAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("did not connect to master: %v", err)
	}
	defer conn.Close()

	app := &ClientApp{master: pb.NewMasterTrackerClient(conn)}

	http.HandleFunc("/", app.ServeGUI)
	http.HandleFunc("/api/upload", app.HandleUpload)
	http.HandleFunc("/api/download", app.HandleDownload)
	http.HandleFunc("/api/files", app.HandleListFiles)
	http.HandleFunc("/api/delete", app.HandleDelete)
	http.HandleFunc("/api/delete_all", app.HandleDeleteAll)
	http.HandleFunc("/nexus.jpg", func(w http.ResponseWriter, r *http.Request) {
		http.ServeFile(w, r, "nexus.jpg")
	})

	log.Printf("Client GUI available at http://localhost:%s", *httpPort)
	if err := http.ListenAndServe(":"+*httpPort, nil); err != nil {
		log.Fatalf("HTTP server failed: %v", err)
	}
}

func (app *ClientApp) ServeGUI(w http.ResponseWriter, r *http.Request) {
	http.ServeFile(w, r, "client/client_gui.html")
}

func (app *ClientApp) HandleListFiles(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(context.Background(), clientMasterRPCTimeout)
	defer cancel()

	resp, err := app.master.ListFiles(ctx, &pb.ListFilesRequest{})
	if err != nil {
		if st, ok := status.FromError(err); ok {
			http.Error(w, st.Message(), grpcCodeToHTTP(st.Code()))
			return
		}
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(resp); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

func (app *ClientApp) HandleUpload(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	file, header, err := r.FormFile("file")
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	defer file.Close()

	fileName := filepath.Base(strings.TrimSpace(header.Filename))
	if fileName == "" {
		http.Error(w, "file name is required", http.StatusBadRequest)
		return
	}
	if !strings.EqualFold(filepath.Ext(fileName), allowedUploadSuffix) {
		http.Error(w, "only .mp4 uploads are supported", http.StatusBadRequest)
		return
	}

	requestCtx, cancel := context.WithTimeout(context.Background(), clientMasterRPCTimeout)
	uploadTarget, err := app.master.RequestUpload(requestCtx, &pb.UploadRequest{
		FileName: fileName,
		FileSize: header.Size,
	})
	cancel()
	if err != nil {
		if st, ok := status.FromError(err); ok {
			http.Error(w, st.Message(), grpcCodeToHTTP(st.Code()))
			return
		}
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
		return
	}

	conn, err := net.DialTimeout("tcp", net.JoinHostPort(uploadTarget.GetNodeIp(), fmt.Sprintf("%d", uploadTarget.GetNodePort())), dataKeeperDialTimeout)
	if err != nil {
		http.Error(w, "failed to connect to assigned data keeper", http.StatusBadGateway)
		return
	}

	if _, err := conn.Write([]byte{uploadCommand}); err != nil {
		conn.Close()
		http.Error(w, "failed to start upload", http.StatusBadGateway)
		return
	}
	if err := writeSizedString(conn, uploadTarget.GetUploadId()); err != nil {
		conn.Close()
		http.Error(w, "failed to send upload session id", http.StatusBadGateway)
		return
	}
	if err := writeSizedString(conn, fileName); err != nil {
		conn.Close()
		http.Error(w, "failed to send file name", http.StatusBadGateway)
		return
	}
	if _, err := io.Copy(conn, file); err != nil {
		conn.Close()
		http.Error(w, "upload transfer failed", http.StatusBadGateway)
		return
	}
	if err := conn.Close(); err != nil {
		http.Error(w, "failed to finalize upload transfer", http.StatusBadGateway)
		return
	}

	waitCtx, waitCancel := context.WithTimeout(context.Background(), uploadConfirmationWait)
	waitResp, err := app.master.WaitForUpload(waitCtx, &pb.WaitForUploadRequest{
		UploadId: uploadTarget.GetUploadId(),
	})
	waitCancel()
	if err != nil {
		if st, ok := status.FromError(err); ok {
			http.Error(w, st.Message(), grpcCodeToHTTP(st.Code()))
			return
		}
		http.Error(w, err.Error(), http.StatusGatewayTimeout)
		return
	}
	if !waitResp.GetSuccess() {
		http.Error(w, waitResp.GetMessage(), http.StatusBadGateway)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{
		"success": true,
		"message": waitResp.GetMessage(),
	})
}

func (app *ClientApp) HandleDownload(w http.ResponseWriter, r *http.Request) {
	fileName := filepath.Base(strings.TrimSpace(r.URL.Query().Get("name")))
	if fileName == "" {
		http.Error(w, "missing file name", http.StatusBadRequest)
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), clientMasterRPCTimeout)
	resp, err := app.master.RequestDownload(ctx, &pb.DownloadRequest{FileName: fileName})
	cancel()
	if err != nil {
		if st, ok := status.FromError(err); ok {
			http.Error(w, st.Message(), grpcCodeToHTTP(st.Code()))
			return
		}
		http.Error(w, "download lookup failed", http.StatusBadGateway)
		return
	}

	if len(resp.GetLocations()) == 0 {
		http.Error(w, "no alive replicas are available", http.StatusNotFound)
		return
	}

	if resp.GetFileSize() == 0 || len(resp.GetLocations()) == 1 {
		app.streamSingleReplicaDownload(w, fileName, resp)
		return
	}

	chunks, err := app.fetchParallelChunks(fileName, resp)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}

	w.Header().Set("Content-Disposition", "attachment; filename="+fileName)
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Content-Length", fmt.Sprintf("%d", resp.GetFileSize()))

	for _, chunk := range chunks {
		if _, err := w.Write(chunk); err != nil {
			log.Printf("failed to write assembled download for %s: %v", fileName, err)
			return
		}
	}
}

func (app *ClientApp) streamSingleReplicaDownload(w http.ResponseWriter, fileName string, resp *pb.DownloadResponse) {
	location := resp.GetLocations()[0]
	conn, err := net.DialTimeout("tcp", net.JoinHostPort(location.GetIp(), fmt.Sprintf("%d", location.GetPort())), dataKeeperDialTimeout)
	if err != nil {
		http.Error(w, "failed to connect to storage node", http.StatusBadGateway)
		return
	}
	defer conn.Close()

	if _, err := conn.Write([]byte{downloadCommand}); err != nil {
		http.Error(w, "failed to start download", http.StatusBadGateway)
		return
	}
	if err := writeSizedString(conn, fileName); err != nil {
		http.Error(w, "failed to send file name", http.StatusBadGateway)
		return
	}

	w.Header().Set("Content-Disposition", "attachment; filename="+fileName)
	w.Header().Set("Content-Type", "application/octet-stream")
	if resp.GetFileSize() > 0 {
		w.Header().Set("Content-Length", fmt.Sprintf("%d", resp.GetFileSize()))
	}

	if _, err := io.Copy(w, conn); err != nil {
		log.Printf("streamed download for %s ended with error: %v", fileName, err)
	}
}

func (app *ClientApp) fetchParallelChunks(fileName string, resp *pb.DownloadResponse) ([][]byte, error) {
	locations := resp.GetLocations()
	ranges := splitFileIntoRanges(resp.GetFileSize(), len(locations))
	results := make([][]byte, len(locations))
	resultChan := make(chan chunkResult, len(locations))

	var wg sync.WaitGroup
	for index, location := range locations {
		wg.Add(1)
		go func(index int, location *pb.NodeLocation, segment downloadRange) {
			defer wg.Done()
			chunk, err := fetchChunkFromReplica(fileName, location, segment)
			resultChan <- chunkResult{index: index, data: chunk, err: err}
		}(index, location, ranges[index])
	}

	wg.Wait()
	close(resultChan)

	for result := range resultChan {
		if result.err != nil {
			return nil, result.err
		}
		results[result.index] = result.data
	}

	return results, nil
}

func fetchChunkFromReplica(fileName string, location *pb.NodeLocation, segment downloadRange) ([]byte, error) {
	conn, err := net.DialTimeout("tcp", net.JoinHostPort(location.GetIp(), fmt.Sprintf("%d", location.GetPort())), dataKeeperDialTimeout)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to replica %s:%d: %w", location.GetIp(), location.GetPort(), err)
	}
	defer conn.Close()

	if _, err := conn.Write([]byte{rangeCommand}); err != nil {
		return nil, fmt.Errorf("failed to send range command: %w", err)
	}
	if err := writeSizedString(conn, fileName); err != nil {
		return nil, fmt.Errorf("failed to send range file name: %w", err)
	}
	if err := binary.Write(conn, binary.BigEndian, segment.start); err != nil {
		return nil, fmt.Errorf("failed to send range start: %w", err)
	}
	if err := binary.Write(conn, binary.BigEndian, segment.length); err != nil {
		return nil, fmt.Errorf("failed to send range length: %w", err)
	}

	buffer := make([]byte, int(segment.length))
	if _, err := io.ReadFull(conn, buffer); err != nil {
		return nil, fmt.Errorf("failed to read chunk [%d:%d] from %s:%d: %w", segment.start, segment.start+segment.length, location.GetIp(), location.GetPort(), err)
	}

	log.Printf("Downloaded chunk [%d:%d] from %s:%d", segment.start, segment.start+segment.length, location.GetIp(), location.GetPort())
	return buffer, nil
}

func splitFileIntoRanges(fileSize int64, replicas int) []downloadRange {
	ranges := make([]downloadRange, replicas)
	if replicas == 0 {
		return ranges
	}

	base := fileSize / int64(replicas)
	remainder := fileSize % int64(replicas)
	start := int64(0)

	for index := 0; index < replicas; index++ {
		length := base
		if int64(index) < remainder {
			length++
		}
		ranges[index] = downloadRange{start: start, length: length}
		start += length
	}

	return ranges
}

func (app *ClientApp) HandleDelete(w http.ResponseWriter, r *http.Request) {
	fileName := filepath.Base(strings.TrimSpace(r.URL.Query().Get("name")))
	if fileName == "" {
		http.Error(w, "file name is required", http.StatusBadRequest)
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), clientMasterRPCTimeout)
	defer cancel()

	if _, err := app.master.DeleteFile(ctx, &pb.DeleteFileRequest{FileName: fileName}); err != nil {
		http.Error(w, "failed to delete file: "+err.Error(), http.StatusBadGateway)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]bool{"success": true})
}

func (app *ClientApp) HandleDeleteAll(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(context.Background(), clientMasterRPCTimeout)
	defer cancel()

	if _, err := app.master.DeleteAllFiles(ctx, &pb.DeleteAllRequest{}); err != nil {
		http.Error(w, "failed to wipe cluster: "+err.Error(), http.StatusBadGateway)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]bool{"success": true})
}

func writeSizedString(writer io.Writer, value string) error {
	if err := binary.Write(writer, binary.BigEndian, uint32(len(value))); err != nil {
		return err
	}
	_, err := writer.Write([]byte(value))
	return err
}

func grpcCodeToHTTP(code codes.Code) int {
	switch code {
	case codes.InvalidArgument:
		return http.StatusBadRequest
	case codes.NotFound:
		return http.StatusNotFound
	case codes.AlreadyExists:
		return http.StatusConflict
	case codes.Unavailable:
		return http.StatusServiceUnavailable
	case codes.DeadlineExceeded:
		return http.StatusGatewayTimeout
	default:
		return http.StatusBadGateway
	}
}
