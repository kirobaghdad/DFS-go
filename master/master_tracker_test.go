package main

import (
	"context"
	"testing"
	"time"

	pb "dfs-go/proto"
)

func TestRequestUploadRejectsNonMP4(t *testing.T) {
	server := NewMasterTrackerServer()
	server.nodes["node1"] = &NodeStatus{
		NodeID:   "node1",
		IP:       "127.0.0.1",
		Port:     7001,
		IsAlive:  true,
		LastSeen: time.Now(),
	}

	_, err := server.RequestUpload(context.Background(), &pb.UploadRequest{
		FileName: "notes.txt",
		FileSize: 11,
	})
	if err == nil {
		t.Fatalf("expected non-mp4 upload to be rejected")
	}
}

func TestBuildReplicationTasksSchedulesAllMissingCopiesInOnePass(t *testing.T) {
	server := NewMasterTrackerServer()
	now := time.Now()

	server.nodes["node1"] = &NodeStatus{NodeID: "node1", IP: "10.0.0.1", Port: 7001, IsAlive: true, LastSeen: now}
	server.nodes["node2"] = &NodeStatus{NodeID: "node2", IP: "10.0.0.2", Port: 7002, IsAlive: true, LastSeen: now}
	server.nodes["node3"] = &NodeStatus{NodeID: "node3", IP: "10.0.0.3", Port: 7003, IsAlive: true, LastSeen: now}
	server.upsertFileRecordLocked(FileRecord{
		FileName: "video.mp4",
		NodeID:   "node1",
		FilePath: "data_node1/video.mp4",
		FileSize: 32,
	})

	tasks := server.buildReplicationTasks()
	if len(tasks) != 2 {
		t.Fatalf("expected 2 replication tasks, got %d", len(tasks))
	}

	destinations := map[string]bool{}
	for _, task := range tasks {
		destinations[task.Dest.NodeID] = true
		if task.Source.NodeID != "node1" {
			t.Fatalf("expected node1 to be the source, got %s", task.Source.NodeID)
		}
		if task.TransferID == "" {
			t.Fatalf("expected replication task to reserve a transfer ID")
		}
	}

	if !destinations["node2"] || !destinations["node3"] {
		t.Fatalf("expected tasks for node2 and node3, got %+v", destinations)
	}
}

func TestBuildReplicationTasksSkipsDestinationWithActiveTransfer(t *testing.T) {
	server := NewMasterTrackerServer()
	now := time.Now()

	server.nodes["node1"] = &NodeStatus{NodeID: "node1", IP: "10.0.0.1", Port: 7001, IsAlive: true, LastSeen: now}
	server.nodes["node2"] = &NodeStatus{NodeID: "node2", IP: "10.0.0.2", Port: 7002, IsAlive: true, LastSeen: now}
	server.nodes["node3"] = &NodeStatus{NodeID: "node3", IP: "10.0.0.3", Port: 7003, IsAlive: true, LastSeen: now}
	server.upsertFileRecordLocked(FileRecord{
		FileName: "video.mp4",
		NodeID:   "node1",
		FilePath: "data_node1/video.mp4",
		FileSize: 32,
	})

	server.transfers["transfer-1"] = &TransferProgress{
		TransferID:        "transfer-1",
		FileName:          "video.mp4",
		SourceNodeID:      "node1",
		DestinationNodeID: "node2",
		BytesTransferred:  16,
		TotalBytes:        32,
		Status:            transferStatusRunning,
		Message:           "in progress",
		StartedAt:         now,
		UpdatedAt:         now,
		Active:            true,
	}
	server.activeTransfers[replicationKey("video.mp4", "node2")] = "transfer-1"

	tasks := server.buildReplicationTasks()
	if len(tasks) != 1 {
		t.Fatalf("expected one remaining replication task, got %d", len(tasks))
	}
	if tasks[0].Dest.NodeID != "node3" {
		t.Fatalf("expected node3 to be scheduled next, got %s", tasks[0].Dest.NodeID)
	}
}

func TestFileHealthStaysUnderReplicatedWhenOnlyTwoNodesAreAlive(t *testing.T) {
	server := NewMasterTrackerServer()
	now := time.Now()

	server.nodes["node1"] = &NodeStatus{NodeID: "node1", IP: "10.0.0.1", Port: 7001, IsAlive: true, LastSeen: now}
	server.nodes["node2"] = &NodeStatus{NodeID: "node2", IP: "10.0.0.2", Port: 7002, IsAlive: true, LastSeen: now}
	server.upsertFileRecordLocked(FileRecord{
		FileName: "video.mp4",
		NodeID:   "node1",
		FilePath: "data_node1/video.mp4",
		FileSize: 32,
	})
	server.upsertFileRecordLocked(FileRecord{
		FileName: "video.mp4",
		NodeID:   "node2",
		FilePath: "data_node2/video.mp4",
		FileSize: 32,
	})

	server.mu.RLock()
	health := server.buildFileHealthLocked()
	server.mu.RUnlock()

	if len(health) != 1 {
		t.Fatalf("expected one file health record, got %d", len(health))
	}
	if health[0].FullyReplicated {
		t.Fatalf("expected file to remain under-replicated with only two alive nodes")
	}
	if health[0].AliveReplicas != 2 {
		t.Fatalf("expected 2 alive replicas, got %d", health[0].AliveReplicas)
	}
}

func TestNotifyUploadRejectsIncompleteClientTransfer(t *testing.T) {
	server := NewMasterTrackerServer()
	now := time.Now()
	server.nodes["node1"] = &NodeStatus{NodeID: "node1", IP: "10.0.0.1", Port: 7001, IsAlive: true, LastSeen: now}
	server.pendingUploads["upload-1"] = &UploadSession{
		ID:           "upload-1",
		FileName:     "video.mp4",
		FileSize:     100,
		TargetNodeID: "node1",
		CreatedAt:    now,
		done:         make(chan struct{}),
	}

	_, err := server.NotifyUpload(context.Background(), &pb.NotifyUploadRequest{
		UploadId: "upload-1",
		FileName: "video.mp4",
		NodeId:   "node1",
		FilePath: "data_node1/video.mp4",
		FileSize: 60,
	})
	if err == nil {
		t.Fatalf("expected incomplete upload to be rejected")
	}

	session := server.pendingUploads["upload-1"]
	if session == nil {
		t.Fatalf("expected upload session to remain available")
	}
	if session.Success {
		t.Fatalf("expected failed upload session")
	}
	if session.CompletedAt.IsZero() {
		t.Fatalf("expected failed upload session to be completed")
	}
	if records := server.fileIndex["video.mp4"]; len(records) != 0 {
		t.Fatalf("expected incomplete upload to stay out of file index")
	}
}

func TestNotifyUploadMarksReplicationCompleted(t *testing.T) {
	server := NewMasterTrackerServer()
	now := time.Now()
	server.nodes["node1"] = &NodeStatus{NodeID: "node1", IP: "10.0.0.1", Port: 7001, IsAlive: true, LastSeen: now}
	server.nodes["node2"] = &NodeStatus{NodeID: "node2", IP: "10.0.0.2", Port: 7002, IsAlive: true, LastSeen: now}
	server.upsertFileRecordLocked(FileRecord{
		FileName: "video.mp4",
		NodeID:   "node1",
		FilePath: "data_node1/video.mp4",
		FileSize: 32,
	})
	server.transfers["transfer-1"] = &TransferProgress{
		TransferID:        "transfer-1",
		FileName:          "video.mp4",
		SourceNodeID:      "node1",
		DestinationNodeID: "node2",
		BytesTransferred:  32,
		TotalBytes:        32,
		Status:            transferStatusAwaitingConfirmation,
		Message:           "waiting for destination confirmation",
		StartedAt:         now,
		UpdatedAt:         now,
		Active:            true,
	}
	server.activeTransfers[replicationKey("video.mp4", "node2")] = "transfer-1"

	_, err := server.NotifyUpload(context.Background(), &pb.NotifyUploadRequest{
		FileName: "video.mp4",
		NodeId:   "node2",
		FilePath: "data_node2/video.mp4",
		FileSize: 32,
	})
	if err != nil {
		t.Fatalf("expected replication notify to succeed: %v", err)
	}

	transfer := server.transfers["transfer-1"]
	if transfer == nil {
		t.Fatalf("expected transfer state to remain available")
	}
	if transfer.Active {
		t.Fatalf("expected transfer to become inactive after confirmation")
	}
	if transfer.Status != transferStatusCompleted {
		t.Fatalf("expected transfer to be completed, got %s", transfer.Status)
	}
	if _, exists := server.activeTransfers[replicationKey("video.mp4", "node2")]; exists {
		t.Fatalf("expected active transfer key to be cleared")
	}
}
