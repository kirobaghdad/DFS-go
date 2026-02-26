package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"path/filepath"
	"time"

	pb "dfs-go/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type DataKeeper struct {
	pb.UnimplementedDataKeeperServer
	id      string
	ip      string
	tcpPort int32
	master  pb.MasterTrackerClient
	baseDir string
}

func NewDataKeeper(id, ip string, tcpPort int32, master pb.MasterTrackerClient) *DataKeeper {
	baseDir := fmt.Sprintf("data_%s", id)
	os.MkdirAll(baseDir, 0755)
	return &DataKeeper{
		id:      id,
		ip:      ip,
		tcpPort: tcpPort,
		master:  master,
		baseDir: baseDir,
	}
}

func (dk *DataKeeper) SendHeartbeat() {
	// First sync existing files
	dk.SyncExistingFiles()

	ticker := time.NewTicker(1 * time.Second)
	for range ticker.C {
		_, err := dk.master.Heartbeat(context.Background(), &pb.HeartbeatRequest{
			NodeId: dk.id,
			Ip:     dk.ip,
			Port:   dk.tcpPort,
		})
		if err != nil {
			log.Printf("Failed to send heartbeat: %v", err)
		}
	}
}

func (dk *DataKeeper) SyncExistingFiles() {
	files, err := os.ReadDir(dk.baseDir)
	if err != nil {
		log.Printf("Failed to read base directory %s: %v", dk.baseDir, err)
		return
	}

	var fileInfos []*pb.FileInfo
	for _, f := range files {
		if !f.IsDir() {
			filePath := filepath.Join(dk.baseDir, f.Name())
			info, err := os.Stat(filePath)
			if err != nil {
				continue
			}
			fileInfos = append(fileInfos, &pb.FileInfo{
				FileName: f.Name(),
				FileSize: info.Size(),
				FilePath: filePath,
			})
		}
	}

	if len(fileInfos) > 0 {
		_, err := dk.master.ReportFiles(context.Background(), &pb.ReportFilesRequest{
			NodeId: dk.id,
			Files:  fileInfos,
		})
		if err != nil {
			log.Printf("Failed to report existing files to master: %v", err)
		} else {
			log.Printf("Synchronized %d existing files with master", len(fileInfos))
		}
	}
}

// StartTCPServer handles direct file transfers from clients and other nodes
func (dk *DataKeeper) StartTCPServer() {
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", dk.tcpPort))
	if err != nil {
		log.Fatalf("Failed to listen on TCP: %v", err)
	}
	log.Printf("Data Keeper TCP server listening on :%d", dk.tcpPort)

	for {
		conn, err := lis.Accept()
		if err != nil {
			log.Printf("Failed to accept connection: %v", err)
			continue
		}
		go dk.handleFileTransfer(conn)
	}
}

func (dk *DataKeeper) handleFileTransfer(conn net.Conn) {
	defer conn.Close()

	// Protocol: [COMMAND (1 byte)][NAME_LEN (4 bytes)][NAME][FILE_CONTENT if upload]
	// COMMAND: 0x01 = UPLOAD, 0x02 = DOWNLOAD

	cmdBuf := make([]byte, 1)
	if _, err := io.ReadFull(conn, cmdBuf); err != nil {
		log.Printf("Failed to read command: %v", err)
		return
	}
	cmd := cmdBuf[0]

	header := make([]byte, 4)
	if _, err := io.ReadFull(conn, header); err != nil {
		log.Printf("Failed to read name length: %v", err)
		return
	}

	nameLen := int(uint32(header[0])<<24 | uint32(header[1])<<16 | uint32(header[2])<<8 | uint32(header[3]))
	nameBuf := make([]byte, nameLen)
	if _, err := io.ReadFull(conn, nameBuf); err != nil {
		log.Printf("Failed to read file name: %v", err)
		return
	}
	fileName := string(nameBuf)

	if cmd == 0x01 { // UPLOAD
		filePath := filepath.Join(dk.baseDir, fileName)
		file, err := os.Create(filePath)
		if err != nil {
			log.Printf("Failed to create file %s: %v", filePath, err)
			return
		}
		defer file.Close()

		if _, err := io.Copy(file, conn); err != nil {
			log.Printf("Failed to save file content: %v", err)
			return
		}

		log.Printf("File %s uploaded/replicated to %s", fileName, filePath)

		// Notify Master Tracker
		fileInfo, _ := file.Stat()
		_, err = dk.master.NotifyUpload(context.Background(), &pb.NotifyUploadRequest{
			FileName: fileName,
			NodeId:   dk.id,
			FilePath: filePath,
			FileSize: fileInfo.Size(),
		})
		if err != nil {
			log.Printf("Failed to notify master: %v", err)
		}
	} else if cmd == 0x02 { // DOWNLOAD
		filePath := filepath.Join(dk.baseDir, fileName)
		file, err := os.Open(filePath)
		if err != nil {
			log.Printf("Failed to open file %s for download: %v", filePath, err)
			return
		}
		defer file.Close()

		if _, err := io.Copy(conn, file); err != nil {
			log.Printf("Failed to send file content: %v", err)
		}
		log.Printf("File %s served for download", fileName)
	} else if cmd == 0x03 { // RANGE DOWNLOAD
		// Protocol: [CMD=0x03][NAME_LEN][NAME][START_OFFSET (8 bytes)][LENGTH (8 bytes)]
		rangeBuf := make([]byte, 16)
		if _, err := io.ReadFull(conn, rangeBuf); err != nil {
			log.Printf("Failed to read range: %v", err)
			return
		}

		start := int64(uint64(rangeBuf[0])<<56 | uint64(rangeBuf[1])<<48 | uint64(rangeBuf[2])<<40 | uint64(rangeBuf[3])<<32 |
			uint64(rangeBuf[4])<<24 | uint64(rangeBuf[5])<<16 | uint64(rangeBuf[6])<<8 | uint64(rangeBuf[7]))
		length := int64(uint64(rangeBuf[8])<<56 | uint64(rangeBuf[9])<<48 | uint64(rangeBuf[10])<<40 | uint64(rangeBuf[11])<<32 |
			uint64(rangeBuf[12])<<24 | uint64(rangeBuf[13])<<16 | uint64(rangeBuf[14])<<8 | uint64(rangeBuf[15]))

		filePath := filepath.Join(dk.baseDir, fileName)
		file, err := os.Open(filePath)
		if err != nil {
			log.Printf("Failed to open file %s for range download: %v", filePath, err)
			return
		}
		defer file.Close()

		file.Seek(start, 0)
		if _, err := io.CopyN(conn, file, length); err != nil {
			log.Printf("Failed to send range content: %v", err)
		}
		log.Printf("File %s served range [%d:%d]", fileName, start, start+length)
	}
}

func (dk *DataKeeper) DeleteFile(ctx context.Context, req *pb.DeleteFileRequest) (*pb.DeleteFileResponse, error) {
	filePath := filepath.Join(dk.baseDir, req.FileName)
	err := os.Remove(filePath)
	if err != nil {
		if os.IsNotExist(err) {
			log.Printf("File %s already deleted locally", req.FileName)
			return &pb.DeleteFileResponse{Success: true}, nil
		}
		log.Printf("Failed to delete local file %s: %v", filePath, err)
		return &pb.DeleteFileResponse{Success: false}, err
	}
	log.Printf("File %s deleted locally", req.FileName)
	return &pb.DeleteFileResponse{Success: true}, nil
}

func (dk *DataKeeper) Wipe(ctx context.Context, req *pb.WipeRequest) (*pb.WipeResponse, error) {
	files, err := os.ReadDir(dk.baseDir)
	if err != nil {
		log.Printf("Wipe: Failed to read base directory %s: %v", dk.baseDir, err)
		return &pb.WipeResponse{Success: false}, err
	}

	for _, f := range files {
		if !f.IsDir() {
			err := os.Remove(filepath.Join(dk.baseDir, f.Name()))
			if err != nil {
				log.Printf("Wipe: Failed to remove file %s: %v", f.Name(), err)
			}
		}
	}

	log.Printf("Wipe: All files cleared from node disk")
	return &pb.WipeResponse{Success: true}, nil
}

func (dk *DataKeeper) Replicate(ctx context.Context, req *pb.ReplicateRequest) (*pb.ReplicateResponse, error) {
	log.Printf("Replicating %s to %s:%d", req.FileName, req.DestinationIp, req.DestinationPort)

	filePath := filepath.Join(dk.baseDir, req.FileName)
	file, err := os.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to open file for replication: %v", err)
	}
	defer file.Close()

	conn, err := net.Dial("tcp", net.JoinHostPort(req.DestinationIp, fmt.Sprintf("%d", req.DestinationPort)))
	if err != nil {
		return nil, fmt.Errorf("failed to connect to destination for replication: %v", err)
	}
	defer conn.Close()

	// Send [CMD][NAME_LEN][NAME][CONTENT]
	name := req.FileName
	nameLen := uint32(len(name))
	header := []byte{0x01, byte(nameLen >> 24), byte(nameLen >> 16), byte(nameLen >> 8), byte(nameLen)}
	conn.Write(header)
	conn.Write([]byte(name))
	io.Copy(conn, file)

	return &pb.ReplicateResponse{Success: true}, nil
}

func (dk *DataKeeper) StartRPCServer(port int32) {
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		log.Fatalf("failed to listen for gRPC: %v", err)
	}
	s := grpc.NewServer()
	pb.RegisterDataKeeperServer(s, dk)
	log.Printf("Data Keeper gRPC server listening on :%d", port)
	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to serve gRPC: %v", err)
	}
}

func main() {
	id := flag.String("id", "node1", "Node ID")
	tcpPort := flag.Int("port", 7000, "TCP port for file transfer")
	masterAddr := flag.String("master", "localhost:50051", "Master Tracker address")
	flag.Parse()

	conn, err := grpc.NewClient(*masterAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("did not connect to master: %v", err)
	}
	defer conn.Close()
	masterClient := pb.NewMasterTrackerClient(conn)

	ip := "127.0.0.1" // In a real system, get actual IP
	dk := NewDataKeeper(*id, ip, int32(*tcpPort), masterClient)

	go dk.SendHeartbeat()
	go dk.StartTCPServer()

	// Master expects DataKeeper gRPC at tcpPort + 1000
	dk.StartRPCServer(int32(*tcpPort) + 1000)
}
