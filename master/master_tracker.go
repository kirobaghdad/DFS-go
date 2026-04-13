package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"path/filepath"
	"slices"
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
	desiredReplicaCount     = 3
	heartbeatGracePeriod    = 5 * time.Second
	heartbeatMonitorEvery   = 1 * time.Second
	replicationCheckEvery   = 10 * time.Second
	masterRPCDialTimeout    = 10 * time.Second
	completedUploadRetain   = 1 * time.Minute
	completedTransferRetain = 1 * time.Minute
	stalledTransferTimeout  = 2 * time.Minute
	supportedUploadFileType = ".mp4"
	maxDashboardTransfers   = 12

	transferStatusUnknown              = "Unknown"
	transferStatusQueued               = "Queued"
	transferStatusRunning              = "Running"
	transferStatusAwaitingConfirmation = "Awaiting Confirmation"
	transferStatusCompleted            = "Completed"
	transferStatusFailed               = "Failed"
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

type UploadSession struct {
	ID           string
	FileName     string
	FileSize     int64
	TargetNodeID string
	CreatedAt    time.Time
	CompletedAt  time.Time
	Success      bool
	Message      string
	done         chan struct{}
}

type FilePlacement struct {
	NodeID string `json:"node_id"`
	Alive  bool   `json:"alive"`
}

type FileHealth struct {
	FileName          string          `json:"file_name"`
	FileSize          int64           `json:"file_size"`
	AliveReplicas     int             `json:"alive_replicas"`
	KnownReplicas     int             `json:"known_replicas"`
	DesiredReplicas   int             `json:"desired_replicas"`
	FullyReplicated   bool            `json:"fully_replicated"`
	UnderReplicated   bool            `json:"under_replicated"`
	MissingReplicas   int             `json:"missing_replicas"`
	Placements        []FilePlacement `json:"placements"`
	AliveEnoughToRead bool            `json:"alive_enough_to_read"`
}

type ClusterMetrics struct {
	AliveNodes             int `json:"alive_nodes"`
	TotalNodes             int `json:"total_nodes"`
	DesiredReplicaCount    int `json:"desired_replica_count"`
	TotalFiles             int `json:"total_files"`
	ActiveTransfers        int `json:"active_transfers"`
	FullyReplicatedFiles   int `json:"fully_replicated_files"`
	UnderReplicatedFiles   int `json:"under_replicated_files"`
	FilesWithAliveReplica  int `json:"files_with_alive_replica"`
	FilesWithoutAliveNodes int `json:"files_without_alive_nodes"`
}

type TransferProgress struct {
	TransferID        string    `json:"transfer_id"`
	FileName          string    `json:"file_name"`
	SourceNodeID      string    `json:"source_node_id"`
	DestinationNodeID string    `json:"destination_node_id"`
	BytesTransferred  int64     `json:"bytes_transferred"`
	TotalBytes        int64     `json:"total_bytes"`
	Status            string    `json:"status"`
	Message           string    `json:"message"`
	StartedAt         time.Time `json:"started_at"`
	UpdatedAt         time.Time `json:"updated_at"`
	PercentComplete   float64   `json:"percent_complete"`
	Active            bool      `json:"active"`
}

type DashboardStatus struct {
	Nodes     []NodeStatus       `json:"nodes"`
	Files     []FileHealth       `json:"files"`
	Transfers []TransferProgress `json:"transfers"`
	Metrics   ClusterMetrics     `json:"metrics"`
}

type replicationTask struct {
	TransferID string
	FileName   string
	FileSize   int64
	Source     NodeStatus
	Dest       NodeStatus
}

type MasterTrackerServer struct {
	pb.UnimplementedMasterTrackerServer

	mu              sync.RWMutex
	fileIndex       map[string]map[string]FileRecord
	nodes           map[string]*NodeStatus
	pendingUploads  map[string]*UploadSession
	transfers       map[string]*TransferProgress
	activeTransfers map[string]string
}

func NewMasterTrackerServer() *MasterTrackerServer {
	return &MasterTrackerServer{
		fileIndex:       make(map[string]map[string]FileRecord),
		nodes:           make(map[string]*NodeStatus),
		pendingUploads:  make(map[string]*UploadSession),
		transfers:       make(map[string]*TransferProgress),
		activeTransfers: make(map[string]string),
	}
}

func (s *MasterTrackerServer) Heartbeat(ctx context.Context, req *pb.HeartbeatRequest) (*pb.HeartbeatResponse, error) {
	if req.GetNodeId() == "" {
		return nil, status.Error(codes.InvalidArgument, "node_id is required")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	node, exists := s.nodes[req.GetNodeId()]
	if !exists {
		node = &NodeStatus{NodeID: req.GetNodeId()}
		s.nodes[req.GetNodeId()] = node
	}

	node.IP = req.GetIp()
	node.Port = req.GetPort()
	node.LastSeen = time.Now()
	node.IsAlive = true

	return &pb.HeartbeatResponse{Success: true}, nil
}

func (s *MasterTrackerServer) RequestUpload(ctx context.Context, req *pb.UploadRequest) (*pb.UploadResponse, error) {
	fileName := strings.TrimSpace(req.GetFileName())
	if fileName == "" {
		return nil, status.Error(codes.InvalidArgument, "file_name is required")
	}
	if !strings.EqualFold(filepath.Ext(fileName), supportedUploadFileType) {
		return nil, status.Errorf(codes.InvalidArgument, "only %s files are supported", supportedUploadFileType)
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if records := s.fileIndex[fileName]; len(records) > 0 {
		return nil, status.Error(codes.AlreadyExists, "file already exists; delete it before uploading again")
	}

	node, err := s.selectUploadNodeLocked()
	if err != nil {
		return nil, status.Error(codes.Unavailable, err.Error())
	}

	uploadID, err := newUploadID()
	if err != nil {
		return nil, status.Error(codes.Internal, "failed to create upload session")
	}

	s.pendingUploads[uploadID] = &UploadSession{
		ID:           uploadID,
		FileName:     fileName,
		FileSize:     req.GetFileSize(),
		TargetNodeID: node.NodeID,
		CreatedAt:    time.Now(),
		done:         make(chan struct{}),
	}

	return &pb.UploadResponse{
		UploadId: uploadID,
		NodeIp:   node.IP,
		NodePort: node.Port,
	}, nil
}

func (s *MasterTrackerServer) WaitForUpload(ctx context.Context, req *pb.WaitForUploadRequest) (*pb.WaitForUploadResponse, error) {
	uploadID := strings.TrimSpace(req.GetUploadId())
	if uploadID == "" {
		return nil, status.Error(codes.InvalidArgument, "upload_id is required")
	}

	s.mu.RLock()
	session, exists := s.pendingUploads[uploadID]
	if !exists {
		s.mu.RUnlock()
		return nil, status.Error(codes.NotFound, "upload session not found")
	}
	done := session.done
	s.mu.RUnlock()

	select {
	case <-done:
	case <-ctx.Done():
		return nil, status.Error(codes.DeadlineExceeded, "timed out while waiting for upload confirmation")
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	session, exists = s.pendingUploads[uploadID]
	if !exists {
		return nil, status.Error(codes.NotFound, "upload session expired")
	}

	return &pb.WaitForUploadResponse{
		Success: session.Success,
		Message: session.Message,
	}, nil
}

func (s *MasterTrackerServer) RequestDownload(ctx context.Context, req *pb.DownloadRequest) (*pb.DownloadResponse, error) {
	fileName := strings.TrimSpace(req.GetFileName())
	if fileName == "" {
		return nil, status.Error(codes.InvalidArgument, "file_name is required")
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	records, exists := s.fileIndex[fileName]
	if !exists || len(records) == 0 {
		return nil, status.Error(codes.NotFound, "file not found")
	}

	nodeIDs := mapsKeys(records)
	slices.Sort(nodeIDs)

	var (
		fileSize  int64
		locations []*pb.NodeLocation
	)

	for _, nodeID := range nodeIDs {
		record := records[nodeID]
		fileSize = record.FileSize
		node, exists := s.nodes[nodeID]
		if exists && node.IsAlive {
			locations = append(locations, &pb.NodeLocation{
				Ip:   node.IP,
				Port: node.Port,
			})
		}
	}

	if len(locations) == 0 {
		return nil, status.Error(codes.Unavailable, "file exists but no alive data keeper currently has it")
	}

	return &pb.DownloadResponse{
		Locations: locations,
		FileSize:  fileSize,
	}, nil
}

func (s *MasterTrackerServer) ListFiles(ctx context.Context, req *pb.ListFilesRequest) (*pb.ListFilesResponse, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	health := s.buildFileHealthLocked()
	files := make([]*pb.FileSummary, 0, len(health))
	for _, item := range health {
		files = append(files, &pb.FileSummary{
			FileName:        item.FileName,
			FileSize:        item.FileSize,
			AliveReplicas:   int32(item.AliveReplicas),
			FullyReplicated: item.FullyReplicated,
		})
	}

	return &pb.ListFilesResponse{Files: files}, nil
}

func (s *MasterTrackerServer) ReportTransferProgress(ctx context.Context, req *pb.TransferProgressRequest) (*pb.TransferProgressResponse, error) {
	transferID := strings.TrimSpace(req.GetTransferId())
	if transferID == "" {
		return nil, status.Error(codes.InvalidArgument, "transfer_id is required")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	transfer, exists := s.transfers[transferID]
	if !exists {
		return nil, status.Error(codes.NotFound, "transfer not found")
	}

	if req.GetFileName() != "" {
		transfer.FileName = req.GetFileName()
	}
	if req.GetSourceNodeId() != "" {
		transfer.SourceNodeID = req.GetSourceNodeId()
	}
	if req.GetDestinationNodeId() != "" {
		transfer.DestinationNodeID = req.GetDestinationNodeId()
	}
	if req.GetTotalBytes() > 0 {
		transfer.TotalBytes = req.GetTotalBytes()
	}
	if req.GetBytesTransferred() > transfer.BytesTransferred {
		transfer.BytesTransferred = req.GetBytesTransferred()
	}
	transfer.Message = strings.TrimSpace(req.GetMessage())
	transfer.Status = transferStatusLabel(req.GetStatus())
	transfer.Active = !isTransferTerminal(transfer.Status)
	transfer.UpdatedAt = time.Now()
	if transfer.TotalBytes > 0 {
		transfer.PercentComplete = minFloat64(float64(transfer.BytesTransferred)*100/float64(transfer.TotalBytes), 100)
	}

	if transfer.Status == transferStatusFailed {
		delete(s.activeTransfers, replicationKey(transfer.FileName, transfer.DestinationNodeID))
	}

	return &pb.TransferProgressResponse{Success: true}, nil
}

func (s *MasterTrackerServer) NotifyUpload(ctx context.Context, req *pb.NotifyUploadRequest) (*pb.NotifyUploadResponse, error) {
	fileName := strings.TrimSpace(req.GetFileName())
	nodeID := strings.TrimSpace(req.GetNodeId())
	if fileName == "" || nodeID == "" {
		return nil, status.Error(codes.InvalidArgument, "file_name and node_id are required")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if uploadID := strings.TrimSpace(req.GetUploadId()); uploadID != "" {
		session, exists := s.pendingUploads[uploadID]
		if !exists {
			return nil, status.Error(codes.NotFound, "upload session not found")
		}

		if session.FileName != fileName {
			session.finish(false, fmt.Sprintf("upload rejected: expected file %s but node reported %s", session.FileName, fileName))
			return nil, status.Error(codes.InvalidArgument, "upload file name does not match session")
		}
		if session.TargetNodeID != nodeID {
			session.finish(false, fmt.Sprintf("upload rejected: expected target %s but node reported %s", session.TargetNodeID, nodeID))
			return nil, status.Error(codes.InvalidArgument, "upload node does not match session")
		}
		if session.FileSize != req.GetFileSize() {
			session.finish(false, fmt.Sprintf("upload incomplete: expected %d byte(s) but received %d", session.FileSize, req.GetFileSize()))
			return nil, status.Error(codes.FailedPrecondition, "upload size does not match session")
		}
	}

	s.upsertFileRecordLocked(FileRecord{
		FileName: fileName,
		NodeID:   nodeID,
		FilePath: req.GetFilePath(),
		FileSize: req.GetFileSize(),
	})

	if uploadID := strings.TrimSpace(req.GetUploadId()); uploadID == "" {
		s.markReplicationConfirmedLocked(fileName, nodeID, req.GetFileSize())
	}

	if uploadID := strings.TrimSpace(req.GetUploadId()); uploadID != "" {
		session, exists := s.pendingUploads[uploadID]
		if exists {
			session.finish(true, fmt.Sprintf("master confirmed %s on %s", fileName, nodeID))
		}
	}

	log.Printf("Master recorded %s on %s", fileName, nodeID)
	return &pb.NotifyUploadResponse{Success: true}, nil
}

func (s *MasterTrackerServer) ReportFiles(ctx context.Context, req *pb.ReportFilesRequest) (*pb.ReportFilesResponse, error) {
	nodeID := strings.TrimSpace(req.GetNodeId())
	if nodeID == "" {
		return nil, status.Error(codes.InvalidArgument, "node_id is required")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	s.removeNodeRecordsLocked(nodeID)
	for _, file := range req.GetFiles() {
		if !strings.EqualFold(filepath.Ext(file.GetFileName()), supportedUploadFileType) {
			continue
		}
		s.upsertFileRecordLocked(FileRecord{
			FileName: file.GetFileName(),
			NodeID:   nodeID,
			FilePath: file.GetFilePath(),
			FileSize: file.GetFileSize(),
		})
	}

	log.Printf("Master synchronized %d local file(s) from %s", len(req.GetFiles()), nodeID)
	return &pb.ReportFilesResponse{Success: true}, nil
}

func (s *MasterTrackerServer) DeleteFile(ctx context.Context, req *pb.DeleteFileRequest) (*pb.DeleteFileResponse, error) {
	fileName := strings.TrimSpace(req.GetFileName())
	if fileName == "" {
		return nil, status.Error(codes.InvalidArgument, "file_name is required")
	}

	s.mu.Lock()
	targets := s.collectDeleteTargetsLocked(fileName)
	delete(s.fileIndex, fileName)
	s.mu.Unlock()

	for _, target := range targets {
		go s.deleteFileOnNode(target, fileName)
	}

	log.Printf("Deleted %s from master registry and notified %d keeper(s)", fileName, len(targets))
	return &pb.DeleteFileResponse{Success: true}, nil
}

func (s *MasterTrackerServer) DeleteAllFiles(ctx context.Context, req *pb.DeleteAllRequest) (*pb.DeleteAllResponse, error) {
	s.mu.Lock()
	targets := make([]NodeStatus, 0, len(s.nodes))
	for _, node := range s.nodes {
		if node.IsAlive {
			targets = append(targets, *node)
		}
	}
	s.fileIndex = make(map[string]map[string]FileRecord)
	s.mu.Unlock()

	for _, target := range targets {
		go s.wipeNode(target)
	}

	log.Printf("Cleared master registry and notified %d alive keeper(s) to wipe local storage", len(targets))
	return &pb.DeleteAllResponse{Success: true}, nil
}

func (s *MasterTrackerServer) MonitorNodes() {
	ticker := time.NewTicker(heartbeatMonitorEvery)
	defer ticker.Stop()

	for range ticker.C {
		s.mu.Lock()
		now := time.Now()
		for _, node := range s.nodes {
			if node.IsAlive && now.Sub(node.LastSeen) > heartbeatGracePeriod {
				node.IsAlive = false
				log.Printf("Node %s marked DOWN", node.NodeID)
			}
		}
		s.cleanupUploadSessionsLocked(now)
		s.cleanupTransfersLocked(now)
		s.mu.Unlock()
	}
}

func (s *MasterTrackerServer) ReplicationManager() {
	ticker := time.NewTicker(replicationCheckEvery)
	defer ticker.Stop()

	for range ticker.C {
		for _, task := range s.buildReplicationTasks() {
			go s.callReplicate(task)
		}
	}
}

func (s *MasterTrackerServer) StatusAPI(w http.ResponseWriter, r *http.Request) {
	s.mu.RLock()
	status := s.buildDashboardStatusLocked()
	s.mu.RUnlock()

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(status); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

func (s *MasterTrackerServer) ServeDashboard(w http.ResponseWriter, r *http.Request) {
	http.ServeFile(w, r, "master/dashboard.html")
}

func (s *MasterTrackerServer) selectUploadNodeLocked() (*NodeStatus, error) {
	var alive []*NodeStatus
	for _, node := range s.nodes {
		if node.IsAlive {
			alive = append(alive, node)
		}
	}
	if len(alive) == 0 {
		return nil, errors.New("no alive data keepers available")
	}

	slices.SortFunc(alive, func(left, right *NodeStatus) int {
		leftLoad := s.nodeLoadLocked(left.NodeID)
		rightLoad := s.nodeLoadLocked(right.NodeID)
		switch {
		case leftLoad < rightLoad:
			return -1
		case leftLoad > rightLoad:
			return 1
		case left.NodeID < right.NodeID:
			return -1
		case left.NodeID > right.NodeID:
			return 1
		default:
			return 0
		}
	})

	return alive[0], nil
}

func (s *MasterTrackerServer) nodeLoadLocked(nodeID string) int {
	load := 0
	for _, holders := range s.fileIndex {
		if _, exists := holders[nodeID]; exists {
			load++
		}
	}
	return load
}

func (s *MasterTrackerServer) upsertFileRecordLocked(record FileRecord) {
	holders, exists := s.fileIndex[record.FileName]
	if !exists {
		holders = make(map[string]FileRecord)
		s.fileIndex[record.FileName] = holders
	}
	holders[record.NodeID] = record
}

func (s *MasterTrackerServer) removeNodeRecordsLocked(nodeID string) {
	for fileName, holders := range s.fileIndex {
		delete(holders, nodeID)
		if len(holders) == 0 {
			delete(s.fileIndex, fileName)
		}
	}
}

func (s *MasterTrackerServer) buildReplicationTasks() []replicationTask {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := time.Now()
	s.cleanupTransfersLocked(now)

	aliveNodes := make([]NodeStatus, 0, len(s.nodes))
	for _, node := range s.nodes {
		if node.IsAlive {
			aliveNodes = append(aliveNodes, *node)
		}
	}
	slices.SortFunc(aliveNodes, func(left, right NodeStatus) int {
		switch {
		case left.NodeID < right.NodeID:
			return -1
		case left.NodeID > right.NodeID:
			return 1
		default:
			return 0
		}
	})

	if len(aliveNodes) == 0 {
		return nil
	}

	fileNames := mapsKeys(s.fileIndex)
	slices.Sort(fileNames)

	var tasks []replicationTask
	for _, fileName := range fileNames {
		holders := s.fileIndex[fileName]
		aliveHolderIDs := make([]string, 0, len(holders))
		var fileSize int64
		for nodeID := range holders {
			fileSize = holders[nodeID].FileSize
			if node, exists := s.nodes[nodeID]; exists && node.IsAlive {
				aliveHolderIDs = append(aliveHolderIDs, nodeID)
			}
		}
		slices.Sort(aliveHolderIDs)

		if len(aliveHolderIDs) == 0 {
			log.Printf("Replication skipped for %s: no alive source keeper currently has the file", fileName)
			continue
		}

		targetReplicaCount := minInt(desiredReplicaCount, len(aliveNodes))
		if len(aliveHolderIDs) >= targetReplicaCount {
			if len(aliveHolderIDs) < desiredReplicaCount {
				log.Printf("File %s remains under-replicated: %d/%d replicas because only %d keeper(s) are alive", fileName, len(aliveHolderIDs), desiredReplicaCount, len(aliveNodes))
			}
			continue
		}

		sourceNode := *s.nodes[aliveHolderIDs[0]]
		plannedHolders := make(map[string]bool, len(aliveHolderIDs))
		for _, nodeID := range aliveHolderIDs {
			plannedHolders[nodeID] = true
		}
		for _, transfer := range s.transfers {
			if transfer.FileName != fileName || !transfer.Active {
				continue
			}
			plannedHolders[transfer.DestinationNodeID] = true
		}

		if len(plannedHolders) >= targetReplicaCount {
			continue
		}

		scheduledCount := 0
		for _, candidate := range aliveNodes {
			if len(plannedHolders) >= targetReplicaCount {
				break
			}
			if plannedHolders[candidate.NodeID] {
				continue
			}
			transferID, err := newTransferID()
			if err != nil {
				log.Printf("Failed to create transfer ID for %s -> %s: %v", fileName, candidate.NodeID, err)
				continue
			}
			plannedHolders[candidate.NodeID] = true
			s.transfers[transferID] = &TransferProgress{
				TransferID:        transferID,
				FileName:          fileName,
				SourceNodeID:      sourceNode.NodeID,
				DestinationNodeID: candidate.NodeID,
				TotalBytes:        fileSize,
				Status:            transferStatusQueued,
				Message:           "queued by master",
				StartedAt:         now,
				UpdatedAt:         now,
				Active:            true,
			}
			s.activeTransfers[replicationKey(fileName, candidate.NodeID)] = transferID
			tasks = append(tasks, replicationTask{
				TransferID: transferID,
				FileName:   fileName,
				FileSize:   fileSize,
				Source:     sourceNode,
				Dest:       candidate,
			})
			scheduledCount++
		}

		if scheduledCount > 0 {
			log.Printf("Replication planner scheduled %d new copy/copies for %s (%d alive replica(s), %d alive keeper(s))", scheduledCount, fileName, len(aliveHolderIDs), len(aliveNodes))
		}
	}

	return tasks
}

func (s *MasterTrackerServer) callReplicate(task replicationTask) {
	ctx, cancel := context.WithTimeout(context.Background(), masterRPCDialTimeout)
	defer cancel()

	conn, err := grpc.NewClient(
		net.JoinHostPort(task.Source.IP, fmt.Sprintf("%d", task.Source.Port+1000)),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		s.mu.Lock()
		s.markTransferFailedLocked(task.TransferID, fmt.Sprintf("failed to connect to source keeper %s: %v", task.Source.NodeID, err))
		s.mu.Unlock()
		log.Printf("Failed to connect to source keeper %s: %v", task.Source.NodeID, err)
		return
	}
	defer conn.Close()

	client := pb.NewDataKeeperClient(conn)
	_, err = client.Replicate(ctx, &pb.ReplicateRequest{
		TransferId:        task.TransferID,
		SourceNodeId:      task.Source.NodeID,
		DestinationNodeId: task.Dest.NodeID,
		FileName:          task.FileName,
		SourceIp:          task.Source.IP,
		SourcePort:        task.Source.Port,
		DestinationIp:     task.Dest.IP,
		DestinationPort:   task.Dest.Port,
	})
	if err != nil {
		s.mu.Lock()
		s.markTransferFailedLocked(task.TransferID, fmt.Sprintf("replication RPC to %s failed: %v", task.Source.NodeID, err))
		s.mu.Unlock()
		log.Printf("Failed to replicate %s from %s to %s: %v", task.FileName, task.Source.NodeID, task.Dest.NodeID, err)
		return
	}

	log.Printf("Replication command sent for %s from %s to %s", task.FileName, task.Source.NodeID, task.Dest.NodeID)
}

func (s *MasterTrackerServer) deleteFileOnNode(node NodeStatus, fileName string) {
	ctx, cancel := context.WithTimeout(context.Background(), masterRPCDialTimeout)
	defer cancel()

	conn, err := grpc.NewClient(
		net.JoinHostPort(node.IP, fmt.Sprintf("%d", node.Port+1000)),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		log.Printf("Delete RPC connection to %s failed: %v", node.NodeID, err)
		return
	}
	defer conn.Close()

	client := pb.NewDataKeeperClient(conn)
	if _, err := client.DeleteFile(ctx, &pb.DeleteFileRequest{FileName: fileName}); err != nil {
		log.Printf("Delete RPC to %s failed: %v", node.NodeID, err)
	}
}

func (s *MasterTrackerServer) wipeNode(node NodeStatus) {
	ctx, cancel := context.WithTimeout(context.Background(), masterRPCDialTimeout)
	defer cancel()

	conn, err := grpc.NewClient(
		net.JoinHostPort(node.IP, fmt.Sprintf("%d", node.Port+1000)),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		log.Printf("Wipe RPC connection to %s failed: %v", node.NodeID, err)
		return
	}
	defer conn.Close()

	client := pb.NewDataKeeperClient(conn)
	if _, err := client.Wipe(ctx, &pb.WipeRequest{}); err != nil {
		log.Printf("Wipe RPC to %s failed: %v", node.NodeID, err)
	}
}

func (s *MasterTrackerServer) collectDeleteTargetsLocked(fileName string) []NodeStatus {
	records := s.fileIndex[fileName]
	if len(records) == 0 {
		return nil
	}

	nodeIDs := mapsKeys(records)
	slices.Sort(nodeIDs)

	targets := make([]NodeStatus, 0, len(nodeIDs))
	for _, nodeID := range nodeIDs {
		if node, exists := s.nodes[nodeID]; exists {
			targets = append(targets, *node)
		}
	}

	return targets
}

func (s *MasterTrackerServer) buildDashboardStatusLocked() DashboardStatus {
	nodes := make([]NodeStatus, 0, len(s.nodes))
	for _, node := range s.nodes {
		nodes = append(nodes, *node)
	}
	slices.SortFunc(nodes, func(left, right NodeStatus) int {
		switch {
		case left.NodeID < right.NodeID:
			return -1
		case left.NodeID > right.NodeID:
			return 1
		default:
			return 0
		}
	})

	files := s.buildFileHealthLocked()
	transfers := s.buildTransferSnapshotLocked()
	metrics := ClusterMetrics{
		DesiredReplicaCount: desiredReplicaCount,
		TotalNodes:          len(nodes),
		TotalFiles:          len(files),
		ActiveTransfers:     s.countActiveTransfersLocked(),
	}

	for _, node := range nodes {
		if node.IsAlive {
			metrics.AliveNodes++
		}
	}

	for _, file := range files {
		if file.FullyReplicated {
			metrics.FullyReplicatedFiles++
		} else {
			metrics.UnderReplicatedFiles++
		}
		if file.AliveReplicas > 0 {
			metrics.FilesWithAliveReplica++
		} else {
			metrics.FilesWithoutAliveNodes++
		}
	}

	return DashboardStatus{
		Nodes:     nodes,
		Files:     files,
		Transfers: transfers,
		Metrics:   metrics,
	}
}

func (s *MasterTrackerServer) buildFileHealthLocked() []FileHealth {
	fileNames := mapsKeys(s.fileIndex)
	slices.Sort(fileNames)

	files := make([]FileHealth, 0, len(fileNames))
	for _, fileName := range fileNames {
		holders := s.fileIndex[fileName]
		nodeIDs := mapsKeys(holders)
		slices.Sort(nodeIDs)

		var (
			fileSize      int64
			aliveReplicas int
			placements    []FilePlacement
		)

		for _, nodeID := range nodeIDs {
			record := holders[nodeID]
			fileSize = record.FileSize
			alive := false
			if node, exists := s.nodes[nodeID]; exists {
				alive = node.IsAlive
			}
			if alive {
				aliveReplicas++
			}
			placements = append(placements, FilePlacement{
				NodeID: nodeID,
				Alive:  alive,
			})
		}

		files = append(files, FileHealth{
			FileName:          fileName,
			FileSize:          fileSize,
			AliveReplicas:     aliveReplicas,
			KnownReplicas:     len(nodeIDs),
			DesiredReplicas:   desiredReplicaCount,
			FullyReplicated:   aliveReplicas >= desiredReplicaCount,
			UnderReplicated:   aliveReplicas < desiredReplicaCount,
			MissingReplicas:   maxInt(desiredReplicaCount-aliveReplicas, 0),
			Placements:        placements,
			AliveEnoughToRead: aliveReplicas > 0,
		})
	}

	return files
}

func (s *MasterTrackerServer) cleanupUploadSessionsLocked(now time.Time) {
	for _, session := range s.pendingUploads {
		if session.CompletedAt.IsZero() {
			continue
		}
		if now.Sub(session.CompletedAt) > completedUploadRetain {
			delete(s.pendingUploads, session.ID)
		}
	}
}

func (s *MasterTrackerServer) cleanupTransfersLocked(now time.Time) {
	for transferID, transfer := range s.transfers {
		if transfer.Active {
			if now.Sub(transfer.UpdatedAt) > stalledTransferTimeout {
				transfer.Active = false
				transfer.Status = transferStatusFailed
				if transfer.Message == "" {
					transfer.Message = "transfer stalled before the destination confirmed the copy"
				}
				delete(s.activeTransfers, replicationKey(transfer.FileName, transfer.DestinationNodeID))
			}
			continue
		}

		if now.Sub(transfer.UpdatedAt) > completedTransferRetain {
			delete(s.transfers, transferID)
		}
	}
}

func (s *MasterTrackerServer) buildTransferSnapshotLocked() []TransferProgress {
	transferIDs := mapsKeys(s.transfers)
	slices.SortFunc(transferIDs, func(left, right string) int {
		leftTransfer := s.transfers[left]
		rightTransfer := s.transfers[right]

		switch {
		case leftTransfer.Active && !rightTransfer.Active:
			return -1
		case !leftTransfer.Active && rightTransfer.Active:
			return 1
		case leftTransfer.UpdatedAt.After(rightTransfer.UpdatedAt):
			return -1
		case leftTransfer.UpdatedAt.Before(rightTransfer.UpdatedAt):
			return 1
		default:
			return strings.Compare(leftTransfer.TransferID, rightTransfer.TransferID)
		}
	})

	transfers := make([]TransferProgress, 0, minInt(len(transferIDs), maxDashboardTransfers))
	for _, transferID := range transferIDs {
		transfers = append(transfers, *s.transfers[transferID])
		if len(transfers) >= maxDashboardTransfers {
			break
		}
	}

	return transfers
}

func (s *MasterTrackerServer) markReplicationConfirmedLocked(fileName, destinationNodeID string, fileSize int64) {
	transferID, exists := s.activeTransfers[replicationKey(fileName, destinationNodeID)]
	if !exists {
		return
	}

	transfer, exists := s.transfers[transferID]
	if !exists {
		delete(s.activeTransfers, replicationKey(fileName, destinationNodeID))
		return
	}

	transfer.BytesTransferred = maxInt64(transfer.BytesTransferred, fileSize)
	transfer.TotalBytes = maxInt64(transfer.TotalBytes, fileSize)
	transfer.PercentComplete = 100
	transfer.Status = transferStatusCompleted
	transfer.Message = fmt.Sprintf("master confirmed %s on %s", fileName, destinationNodeID)
	transfer.UpdatedAt = time.Now()
	transfer.Active = false
	delete(s.activeTransfers, replicationKey(fileName, destinationNodeID))
}

func (s *MasterTrackerServer) markTransferFailedLocked(transferID, message string) {
	transfer, exists := s.transfers[transferID]
	if !exists {
		return
	}

	transfer.Status = transferStatusFailed
	transfer.Message = message
	transfer.UpdatedAt = time.Now()
	transfer.Active = false
	delete(s.activeTransfers, replicationKey(transfer.FileName, transfer.DestinationNodeID))
}

func (s *UploadSession) finish(success bool, message string) {
	if !s.CompletedAt.IsZero() {
		return
	}
	s.Success = success
	s.Message = message
	s.CompletedAt = time.Now()
	close(s.done)
}

func newUploadID() (string, error) {
	buffer := make([]byte, 8)
	if _, err := rand.Read(buffer); err != nil {
		return "", err
	}
	return hex.EncodeToString(buffer), nil
}

func newTransferID() (string, error) {
	return newUploadID()
}

func mapsKeys[T any](input map[string]T) []string {
	keys := make([]string, 0, len(input))
	for key := range input {
		keys = append(keys, key)
	}
	return keys
}

func minInt(left, right int) int {
	if left < right {
		return left
	}
	return right
}

func maxInt(left, right int) int {
	if left > right {
		return left
	}
	return right
}

func maxInt64(left, right int64) int64 {
	if left > right {
		return left
	}
	return right
}

func minFloat64(left, right float64) float64 {
	if left < right {
		return left
	}
	return right
}

func (s *MasterTrackerServer) countActiveTransfersLocked() int {
	count := 0
	for _, transfer := range s.transfers {
		if transfer.Active {
			count++
		}
	}
	return count
}

func replicationKey(fileName, destinationNodeID string) string {
	return fileName + "\x00" + destinationNodeID
}

func transferStatusLabel(status pb.TransferStatus) string {
	switch status {
	case pb.TransferStatus_TRANSFER_STATUS_QUEUED:
		return transferStatusQueued
	case pb.TransferStatus_TRANSFER_STATUS_RUNNING:
		return transferStatusRunning
	case pb.TransferStatus_TRANSFER_STATUS_AWAITING_CONFIRMATION:
		return transferStatusAwaitingConfirmation
	case pb.TransferStatus_TRANSFER_STATUS_COMPLETED:
		return transferStatusCompleted
	case pb.TransferStatus_TRANSFER_STATUS_FAILED:
		return transferStatusFailed
	default:
		return transferStatusUnknown
	}
}

func isTransferTerminal(status string) bool {
	return status == transferStatusCompleted || status == transferStatusFailed
}

func main() {
	grpcListenAddr := flag.String("grpc-listen", ":56051", "gRPC listen address for master tracker")
	dashboardListenAddr := flag.String("dashboard-listen", ":18080", "HTTP dashboard listen address")
	flag.Parse()

	listener, err := net.Listen("tcp", *grpcListenAddr)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	grpcServer := grpc.NewServer()
	server := NewMasterTrackerServer()
	pb.RegisterMasterTrackerServer(grpcServer, server)

	go server.MonitorNodes()
	go server.ReplicationManager()

	http.HandleFunc("/", server.ServeDashboard)
	http.HandleFunc("/api/status", server.StatusAPI)
	http.HandleFunc("/nexus.jpg", func(w http.ResponseWriter, r *http.Request) {
		http.ServeFile(w, r, "nexus.jpg")
	})

	go func() {
		log.Printf("Dashboard listening on %s", *dashboardListenAddr)
		if err := http.ListenAndServe(*dashboardListenAddr, nil); err != nil {
			log.Printf("dashboard server stopped: %v", err)
		}
	}()

	log.Printf("Master Tracker listening on %s", *grpcListenAddr)
	if err := grpcServer.Serve(listener); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
