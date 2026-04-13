package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"sync"
	"time"

	pb "dfs-go/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type FileRecord struct {
	FileName string `json:"file_name"`
	NodeID   string `json:"node_id"`
	FilePath string `json:"file_path"`
	FileSize int64  `json:"file_size"`
}

type NodeStatus struct {
	NodeID   string    `json:"node_id"`
	IP       string    `json:"ip"`
	Port     int32     `json:"port"`
	LastSeen time.Time `json:"last_seen"`
	IsAlive  bool      `json:"is_alive"`
}

type MasterTrackerServer struct {
	pb.UnimplementedMasterTrackerServer
	mu    sync.RWMutex
	files []FileRecord
	nodes map[string]*NodeStatus
}

func NewMasterTrackerServer() *MasterTrackerServer {
	return &MasterTrackerServer{
		nodes: make(map[string]*NodeStatus),
	}
}

func (s *MasterTrackerServer) Heartbeat(ctx context.Context, req *pb.HeartbeatRequest) (*pb.HeartbeatResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	node, exists := s.nodes[req.NodeId]
	if !exists {
		node = &NodeStatus{NodeID: req.NodeId}
		s.nodes[req.NodeId] = node
	}
	node.IP = req.Ip
	node.Port = req.Port
	node.LastSeen = time.Now()
	node.IsAlive = true

	log.Printf("Heartbeat from node %s (%s:%d)", req.NodeId, req.Ip, req.Port)
	return &pb.HeartbeatResponse{Success: true}, nil
}

func (s *MasterTrackerServer) RequestUpload(ctx context.Context, req *pb.UploadRequest) (*pb.UploadResponse, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// Simple load balancing: pick the first alive node
	for _, node := range s.nodes {
		if node.IsAlive {
			return &pb.UploadResponse{
				NodeIp:   node.IP,
				NodePort: node.Port,
			}, nil
		}
	}

	return nil, fmt.Errorf("no alive data keepers available")
}

func (s *MasterTrackerServer) RequestDownload(ctx context.Context, req *pb.DownloadRequest) (*pb.DownloadResponse, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var locations []*pb.NodeLocation
	var fileSize int64
	// Special Case: empty string returns all distinct file names (for GUI listing)
	if req.FileName == "" {
		seen := make(map[string]bool)
		for _, file := range s.files {
			if !seen[file.FileName] {
				locations = append(locations, &pb.NodeLocation{Ip: file.FileName}) // Overloading Ip field for name in this hack
				seen[file.FileName] = true
			}
		}
		return &pb.DownloadResponse{Locations: locations, FileSize: 0}, nil
	}

	for _, file := range s.files {
		if file.FileName == req.FileName {
			fileSize = file.FileSize
			node, exists := s.nodes[file.NodeID]
			if exists && node.IsAlive {
				locations = append(locations, &pb.NodeLocation{
					Ip:   node.IP,
					Port: node.Port,
				})
			}
		}
	}

	return &pb.DownloadResponse{Locations: locations, FileSize: fileSize}, nil
}

func (s *MasterTrackerServer) NotifyUpload(ctx context.Context, req *pb.NotifyUploadRequest) (*pb.NotifyUploadResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.files = append(s.files, FileRecord{
		FileName: req.FileName,
		NodeID:   req.NodeId,
		FilePath: req.FilePath,
		FileSize: req.FileSize,
	})

	log.Printf("File %s (%d bytes) uploaded to node %s", req.FileName, req.FileSize, req.NodeId)
	return &pb.NotifyUploadResponse{Success: true}, nil
}

func (s *MasterTrackerServer) DeleteFile(ctx context.Context, req *pb.DeleteFileRequest) (*pb.DeleteFileResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	var deleteLocations []*pb.NodeLocation
	var remainingFiles []FileRecord

	for _, f := range s.files {
		if f.FileName == req.FileName {
			node, exists := s.nodes[f.NodeID]
			if exists {
				deleteLocations = append(deleteLocations, &pb.NodeLocation{Ip: node.IP, Port: node.Port})
			}
		} else {
			remainingFiles = append(remainingFiles, f)
		}
	}
	s.files = remainingFiles

	// Asynchronously tell nodes to delete files
	for _, loc := range deleteLocations {
		go func(l *pb.NodeLocation) {
			conn, err := grpc.NewClient(net.JoinHostPort(l.Ip, fmt.Sprintf("%d", l.Port+1000)), grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				return
			}
			defer conn.Close()
			client := pb.NewDataKeeperClient(conn)
			client.DeleteFile(context.Background(), &pb.DeleteFileRequest{FileName: req.FileName})
		}(loc)
	}

	log.Printf("File %s deleted from registry, commanded %d nodes to wipe", req.FileName, len(deleteLocations))
	return &pb.DeleteFileResponse{Success: true}, nil
}

func (s *MasterTrackerServer) DeleteAllFiles(ctx context.Context, req *pb.DeleteAllRequest) (*pb.DeleteAllResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	totalFiles := len(s.files)
	s.files = []FileRecord{}

	// Broadcast wipe to all alive nodes
	for _, node := range s.nodes {
		if node.IsAlive {
			go func(n *NodeStatus) {
				conn, err := grpc.NewClient(net.JoinHostPort(n.IP, fmt.Sprintf("%d", n.Port+1000)), grpc.WithTransportCredentials(insecure.NewCredentials()))
				if err != nil {
					return
				}
				defer conn.Close()
				client := pb.NewDataKeeperClient(conn)
				_, err = client.Wipe(context.Background(), &pb.WipeRequest{})
				if err != nil {
					log.Printf("Failed to wipe node %s: %v", n.NodeID, err)
				}
			}(node)
		}
	}

	log.Printf("NEXUS Cluster WIPE: Clear all %d records", totalFiles)
	return &pb.DeleteAllResponse{Success: true}, nil
}

func (s *MasterTrackerServer) ReportFiles(ctx context.Context, req *pb.ReportFilesRequest) (*pb.ReportFilesResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Dedup: remove existing records for this node before adding new ones
	var updatedFiles []FileRecord
	for _, f := range s.files {
		if f.NodeID != req.NodeId {
			updatedFiles = append(updatedFiles, f)
		}
	}
	s.files = updatedFiles

	for _, f := range req.Files {
		s.files = append(s.files, FileRecord{
			FileName: f.FileName,
			NodeID:   req.NodeId,
			FilePath: f.FilePath,
			FileSize: f.FileSize,
		})
	}

	log.Printf("Node %s synchronized %d existing files", req.NodeId, len(req.Files))
	return &pb.ReportFilesResponse{Success: true}, nil
}

// Background thread to check node liveness and trigger replication
func (s *MasterTrackerServer) MonitorNodes() {
	ticker := time.NewTicker(2 * time.Second)
	for range ticker.C {
		s.mu.Lock()
		now := time.Now()
		for id, node := range s.nodes {
			if node.IsAlive && now.Sub(node.LastSeen) > 5*time.Second {
				log.Printf("Node %s is DOWN", id)
				node.IsAlive = false
			}
		}
		s.mu.Unlock()
	}
}

// Replication Manager: awake every 10s as per requirements
func (s *MasterTrackerServer) ReplicationManager() {
	ticker := time.NewTicker(10 * time.Second)
	for range ticker.C {
		s.checkReplication()
	}
}

func (s *MasterTrackerServer) checkReplication() {
	s.mu.RLock()
	defer s.mu.RUnlock()

	fileCounts := make(map[string]int)
	fileHolders := make(map[string][]string) // fileName -> []nodeID

	for _, f := range s.files {
		node, exists := s.nodes[f.NodeID]
		if exists && node.IsAlive {
			fileCounts[f.FileName]++
			fileHolders[f.FileName] = append(fileHolders[f.FileName], f.NodeID)
		}
	}

	for fileName, count := range fileCounts {
		if count < 3 {
			log.Printf("File %s has only %d replicas, triggering replication", fileName, count)
			s.triggerReplication(fileName, fileHolders[fileName])
		}
	}
}

func (s *MasterTrackerServer) triggerReplication(fileName string, holders []string) {
	if len(holders) == 0 {
		return
	}

	// Pick a source node
	sourceID := holders[0]
	sourceNode := s.nodes[sourceID]

	// Find a destination node that doesn't have the file
	var destNode *NodeStatus
	for _, node := range s.nodes {
		if node.IsAlive {
			alreadyHas := false
			for _, holderID := range holders {
				if holderID == node.NodeID {
					alreadyHas = true
					break
				}
			}
			if !alreadyHas {
				destNode = node
				break
			}
		}
	}

	if destNode != nil {
		log.Printf("Instructing node %s to replicate %s to %s", sourceID, fileName, destNode.NodeID)
		go s.callReplicate(sourceNode, destNode, fileName)
	}
}

func (s *MasterTrackerServer) callReplicate(source, dest *NodeStatus, fileName string) {
	conn, err := grpc.NewClient(net.JoinHostPort(source.IP, fmt.Sprintf("%d", source.Port+1000)), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Printf("Failed to connect to source node %s: %v", source.NodeID, err)
		return
	}
	defer conn.Close()

	client := pb.NewDataKeeperClient(conn)
	_, err = client.Replicate(context.Background(), &pb.ReplicateRequest{
		FileName:        fileName,
		SourceIp:        source.IP,
		SourcePort:      source.Port,
		DestinationIp:   dest.IP,
		DestinationPort: dest.Port,
	})
	if err != nil {
		log.Printf("Failed to trigger replication on node %s: %v", source.NodeID, err)
	}
}

func (s *MasterTrackerServer) StatusAPI(w http.ResponseWriter, r *http.Request) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	data := struct {
		Nodes map[string]*NodeStatus `json:"nodes"`
		Files []FileRecord           `json:"files"`
	}{
		Nodes: s.nodes,
		Files: s.files,
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(data)
}

func (s *MasterTrackerServer) ServeDashboard(w http.ResponseWriter, r *http.Request) {
	http.ServeFile(w, r, "master/dashboard.html")
}

func main() {
	grpcListenAddr := flag.String("grpc-listen", ":50051", "gRPC listen address for master tracker")
	dashboardListenAddr := flag.String("dashboard-listen", ":8080", "HTTP dashboard listen address")
	flag.Parse()

	lis, err := net.Listen("tcp", *grpcListenAddr)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	s := grpc.NewServer()
	srv := NewMasterTrackerServer()
	pb.RegisterMasterTrackerServer(s, srv)

	go srv.MonitorNodes()
	go srv.ReplicationManager()

	// Start HTTP Server for Dashboard
	http.HandleFunc("/", srv.ServeDashboard)
	http.HandleFunc("/api/status", srv.StatusAPI)
	http.HandleFunc("/nexus.jpg", func(w http.ResponseWriter, r *http.Request) {
		http.ServeFile(w, r, "nexus.jpg")
	})
	go func() {
		log.Printf("Dashboard listening on %s", *dashboardListenAddr)
		if err := http.ListenAndServe(*dashboardListenAddr, nil); err != nil {
			log.Printf("HTTP server failed: %v", err)
		}
	}()

	log.Printf("Master Tracker listening on %s", *grpcListenAddr)
	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
