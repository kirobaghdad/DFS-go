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
	}

	if !destinations["node2"] || !destinations["node3"] {
		t.Fatalf("expected tasks for node2 and node3, got %+v", destinations)
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
