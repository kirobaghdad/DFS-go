package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"sync"

	pb "dfs-go/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type ClientApp struct {
	master pb.MasterTrackerClient
}

func main() {
	masterAddr := flag.String("master", "localhost:50051", "Master Tracker address")
	httpPort := flag.String("port", "8081", "HTTP port for Client GUI")
	flag.Parse()

	conn, err := grpc.NewClient(*masterAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("did not connect to master: %v", err)
	}
	defer conn.Close()
	masterClient := pb.NewMasterTrackerClient(conn)

	app := &ClientApp{master: masterClient}

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
	resp, err := app.master.RequestDownload(context.Background(), &pb.DownloadRequest{
		FileName: "", // Special case or just list all? Master currently returns only for specific file.
	})
	// Note: Our Master currently only returns locations for a specific file.
	// To list ALL files, we'd need a new RPC.
	// However, for this project, let's assume the Client GUI shows all files known to Master.
	// I'll update the Master's RequestDownload to return all files if fileName is empty.

	if err != nil && err.Error() != "file not found or no alive nodes have it" {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	if resp != nil {
		json.NewEncoder(w).Encode(resp)
	} else {
		fmt.Fprintf(w, `{"Locations": []}`)
	}
}

func (app *ClientApp) HandleUpload(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	file, header, err := r.FormFile("file")
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	defer file.Close()

	fileName := header.Filename

	// 1. Request node from Master
	resp, err := app.master.RequestUpload(context.Background(), &pb.UploadRequest{
		FileName: fileName,
		FileSize: header.Size,
	})
	if err != nil {
		http.Error(w, "Master refused upload: "+err.Error(), http.StatusServiceUnavailable)
		return
	}

	// 2. Connect to Data Keeper
	conn, err := net.Dial("tcp", net.JoinHostPort(resp.NodeIp, fmt.Sprintf("%d", resp.NodePort)))
	if err != nil {
		http.Error(w, "Failed to connect to Data Keeper", http.StatusInternalServerError)
		return
	}
	defer conn.Close()

	// 3. Send [CMD=0x01][NAME_LEN][NAME][CONTENT]
	nameLen := uint32(len(fileName))
	head := []byte{0x01, byte(nameLen >> 24), byte(nameLen >> 16), byte(nameLen >> 8), byte(nameLen)}
	conn.Write(head)
	conn.Write([]byte(fileName))

	_, err = io.Copy(conn, file)
	if err != nil {
		http.Error(w, "Upload failed", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	fmt.Fprintf(w, `{"success": true}`)
}

func (app *ClientApp) HandleDownload(w http.ResponseWriter, r *http.Request) {
	fileName := r.URL.Query().Get("name")
	if fileName == "" {
		http.Error(w, "Missing file name", http.StatusBadRequest)
		return
	}

	// 1. Request locations
	resp, err := app.master.RequestDownload(context.Background(), &pb.DownloadRequest{
		FileName: fileName,
	})
	if err != nil {
		http.Error(w, "File not found", http.StatusNotFound)
		return
	}

	if len(resp.Locations) == 0 {
		http.Error(w, "No copies available", http.StatusNotFound)
		return
	}

	// 2. Parallel Download Logic
	numReplicas := len(resp.Locations)
	fileSize := resp.FileSize
	chunkSize := fileSize / int64(numReplicas)

	// Prepare response
	w.Header().Set("Content-Disposition", "attachment; filename="+fileName)
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Content-Length", fmt.Sprintf("%d", fileSize))

	// If file is empty or only one replica, use simple path
	if fileSize == 0 || numReplicas == 1 {
		loc := resp.Locations[0]
		conn, err := net.Dial("tcp", net.JoinHostPort(loc.Ip, fmt.Sprintf("%d", loc.Port)))
		if err != nil {
			http.Error(w, "Failed to connect to storage node", http.StatusInternalServerError)
			return
		}
		defer conn.Close()

		nameLen := uint32(len(fileName))
		head := []byte{0x02, byte(nameLen >> 24), byte(nameLen >> 16), byte(nameLen >> 8), byte(nameLen)}
		conn.Write(head)
		conn.Write([]byte(fileName))
		io.Copy(w, conn)
		return
	}

	// Parallel Chunk Fetching
	results := make([][]byte, numReplicas)
	var wg sync.WaitGroup
	errChan := make(chan error, numReplicas)

	for i := 0; i < numReplicas; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()

			start := int64(idx) * chunkSize
			length := chunkSize
			if idx == numReplicas-1 {
				length = fileSize - start // Last chunk takes remained
			}

			loc := resp.Locations[idx]
			conn, err := net.Dial("tcp", net.JoinHostPort(loc.Ip, fmt.Sprintf("%d", loc.Port)))
			if err != nil {
				errChan <- fmt.Errorf("node %s failed: %v", loc.Ip, err)
				return
			}
			defer conn.Close()

			// Send [CMD=0x03][NAME_LEN][NAME][START][LEN]
			nameLen := uint32(len(fileName))
			head := []byte{0x03, byte(nameLen >> 24), byte(nameLen >> 16), byte(nameLen >> 8), byte(nameLen)}
			conn.Write(head)
			conn.Write([]byte(fileName))

			rangeHeader := make([]byte, 16)
			for b := 0; b < 8; b++ {
				rangeHeader[b] = byte(start >> (56 - b*8))
				rangeHeader[b+8] = byte(length >> (56 - b*8))
			}
			conn.Write(rangeHeader)

			buf := make([]byte, length)
			_, err = io.ReadFull(conn, buf)
			if err != nil {
				errChan <- fmt.Errorf("failed to read chunk %d: %v", idx, err)
				return
			}
			results[idx] = buf
			log.Printf("Chunk %d [%d:%d] received from %s", idx, start, start+length, loc.Ip)
		}(i)
	}

	wg.Wait()
	close(errChan)

	if len(errChan) > 0 {
		log.Printf("Download had errors, but attempting assembly...")
	}

	for i := 0; i < numReplicas; i++ {
		if results[i] != nil {
			w.Write(results[i])
		}
	}
}

func (app *ClientApp) HandleDelete(w http.ResponseWriter, r *http.Request) {
	fileName := r.URL.Query().Get("name")
	if fileName == "" {
		http.Error(w, "File name is required", http.StatusBadRequest)
		return
	}

	_, err := app.master.DeleteFile(context.Background(), &pb.DeleteFileRequest{FileName: fileName})
	if err != nil {
		http.Error(w, "Failed to delete file: "+err.Error(), http.StatusInternalServerError)
		return
	}

	fmt.Fprintf(w, `{"success": true}`)
}

func (app *ClientApp) HandleDeleteAll(w http.ResponseWriter, r *http.Request) {
	_, err := app.master.DeleteAllFiles(context.Background(), &pb.DeleteAllRequest{})
	if err != nil {
		http.Error(w, "Failed to wipe cluster: "+err.Error(), http.StatusInternalServerError)
		return
	}

	fmt.Fprintf(w, `{"success": true}`)
}
