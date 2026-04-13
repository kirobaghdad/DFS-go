# Nexus DFS Guide

## 1. What I built

I built a centralized distributed file system with three roles:

1. The **Master Tracker** is the coordinator.
2. The **Data Keepers** are the machines that actually store the MP4 files.
3. The **Client** is the user-facing side that uploads and downloads files.

I used **gRPC for control messages** and **raw TCP for file bytes** because that matches the assignment PDF:

- gRPC is used for heartbeats, upload assignment, upload confirmation, file listing, download lookup, replication commands, and the optional delete/wipe extensions.
- TCP is used for the actual file transfer between client and data keeper, and between one data keeper and another during replication.

The whole idea is:

1. The client never chooses a storage node directly.
2. The client first asks the master where to upload.
3. The data keeper stores the file and tells the master it succeeded.
4. The master records the file location.
5. The replication manager keeps checking until every file has the required alive replicas, or honestly reports that the cluster does not currently have enough alive keepers.

---

## 2. Project structure and what each file does

### `proto/dfs.proto`

This file defines the contract between all machines.

The main RPCs are:

- `Heartbeat`: each data keeper calls this every second so the master knows it is alive.
- `RequestUpload`: the client asks the master for a target keeper.
- `WaitForUpload`: the client waits until the master confirms that the keeper really stored the file.
- `RequestDownload`: the client asks the master which alive keepers currently have the file.
- `ListFiles`: the client asks the master for the list of tracked files.
- `NotifyUpload`: a data keeper calls this after it stores an upload or replication copy.
- `ReportFiles`: a data keeper calls this on startup to tell the master what it already has on disk.
- `Replicate`: the master tells a source keeper to copy a file to a destination keeper.

The optional extras are:

- `DeleteFile`
- `DeleteAllFiles`
- `Wipe`

I kept those because they are useful in demos, but I treat them as extensions, not core assignment requirements.

---

### `master/master_tracker.go`

This is the brain of the system.

I changed it so it now keeps a **real file index** instead of a flat list:

- Key 1: file name
- Key 2: node id
- Value: file record

That makes the code much easier to reason about, because each file directly maps to the keepers that hold it.

The master does these jobs:

#### A. Track node liveness

- Every heartbeat updates the node's last seen time and marks it alive.
- A monitor loop runs every second.
- If a node misses heartbeats for more than 5 seconds, the master marks it down.

This is how the dashboard knows which keepers are alive and which are down.

#### B. Assign uploads

When the client calls `RequestUpload`, the master:

1. Checks that the file is an `.mp4`.
2. Picks the **least loaded alive keeper**.
3. Creates an `upload_id`.
4. Returns the keeper IP, keeper TCP port, and the upload id.

I made the master reject duplicate file names so the cluster does not silently hold conflicting versions of the same file.

#### C. Confirm uploads

When the data keeper stores the file, it sends `NotifyUpload`.

The master then:

1. Adds the file record to its index.
2. Marks the waiting upload session as complete.
3. Unblocks `WaitForUpload`.

This means the client only shows success **after the master confirms the upload**, which matches the assignment flow much better than the old optimistic behavior.

#### D. Plan replication

The replication manager wakes up every 10 seconds.

For each distinct file:

1. It finds which alive keepers already hold the file.
2. It picks one alive source keeper.
3. It schedules copies to all missing destinations in the same pass until it reaches:
   `min(3, number of alive keepers)`

Important detail:

- The planner tries to use as many alive keepers as possible.
- But the file is considered **fully replicated only when it has 3 alive replicas**.
- So if only 2 keepers are alive, the file stays readable but is still marked **under-replicated**.

That is the honest behavior I wanted for demos and discussion.

#### E. Serve the dashboard

The dashboard status endpoint returns:

- nodes
- file health
- replica counts
- placement information
- cluster summary metrics

So the dashboard now shows real health information instead of a fake activity feed.

---

### `node/data_keeper.go`

This file is the storage worker.

Each data keeper has:

- a node id
- an advertised IP
- a TCP file-transfer port
- a gRPC control port at `tcpPort + 1000`
- a local directory such as `data_node1`

The data keeper does five main jobs.

#### A. Send heartbeats

Every second it calls `Heartbeat` on the master.

I wrapped the RPC calls in short timeouts so the keeper does not hang forever if the master is unreachable.

#### B. Report local files on startup

When the keeper starts, it scans its local storage folder and sends `ReportFiles` to the master.

This is important because if I restart a keeper, the master can rebuild its file map without re-uploading everything.

#### C. Receive uploads over TCP

For uploads, the TCP protocol is now:

1. command byte = upload
2. upload id length + upload id
3. file name length + file name
4. raw file bytes

The data keeper writes the file to disk, then calls `NotifyUpload`.

If the upload came from the client, the upload id is filled in.
If the upload came from replication, the upload id is empty because no client is waiting on it.

#### D. Serve downloads

The keeper supports:

- full-file download
- range download

Range download is what lets the client split the file across multiple replicas and download chunks in parallel.

#### E. Replicate to another keeper

When the master calls `Replicate`, the source keeper:

1. Opens its local copy.
2. Connects to the destination keeper over TCP.
3. Sends the upload command with an empty upload id.
4. Streams the file.

Then the destination keeper stores the new copy and notifies the master.

---

### `client/client.go`

This file is the user-facing backend that serves the browser UI.

It talks to the master over gRPC and to the keepers over TCP.

#### A. Upload flow

The upload endpoint now follows the assignment much more closely:

1. The browser sends the MP4 to the client HTTP service.
2. The client calls `RequestUpload`.
3. The master returns the chosen keeper plus an upload id.
4. The client sends the file to that keeper over TCP.
5. The keeper stores the file and notifies the master.
6. The client calls `WaitForUpload`.
7. Only after that confirmation does the client return success to the browser.

This fixed the old issue where the browser could say "upload successful" before the master had actually recorded the file.

#### B. File listing

I removed the old hack where `RequestDownload("")` was abused to mean "list files".

Now the client uses the dedicated `ListFiles` RPC.

#### C. Download flow

The download flow is:

1. Ask the master for alive keepers that currently hold the file.
2. If there is only one alive keeper, download the whole file from that keeper.
3. If there are multiple alive keepers, split the file into ranges and request one chunk from each replica.
4. Assemble the chunks in the correct order before sending them back to the browser.

This satisfies the assignment requirement that the client should request from every returned port uniformly.

#### D. Optional delete / wipe

I kept these as convenience tools for testing:

- delete one file from all known replicas
- wipe the cluster

They are useful during demos but are not part of the core upload/download requirement.

---

### `master/dashboard.html`

This page is for the discussion and system visualization.

It now shows:

- how many nodes are alive
- how many files are fully replicated
- how many files are under-replicated
- exact file placements
- whether each holder is alive or down

So when I am asked "what happens if one keeper dies?", I can show the dashboard and explain it visually.

---

### `client/client_gui.html`

This page is the simple user interface.

It lets me:

- upload MP4 files
- see the tracked files
- download a file
- delete a file
- wipe the cluster

I also changed the UI behavior so:

- it clearly says uploads are MP4-only
- it displays real upload success only after master confirmation
- it shows whether a file is fully replicated or still under-replicated

---

### `run_system.sh`

This script is my **single-machine smoke test launcher**.

It now defaults to **3 data keepers**, because that matches the assignment's 3-replica requirement better than the old setup.

It is good for local verification, but my real assignment demo plan is the 2-device setup described below.

---

## 3. How the main flows work, bit by bit

### Upload flow

This is the exact story I would say in discussion:

1. I start from the client.
2. The client does not choose a keeper by itself.
3. It first asks the master tracker for an upload target using gRPC.
4. The master selects the least loaded alive keeper and creates an upload session id.
5. The client opens a TCP connection to that keeper and sends the file.
6. The data keeper writes the file to its local folder.
7. The data keeper tells the master "I stored this file" using `NotifyUpload`.
8. The master updates its lookup table and marks that upload session as completed.
9. The client waits on `WaitForUpload`, so it does not return success early.
10. The replication manager later notices the file if it still needs more replicas.

That matches the assignment idea:

- control plane with gRPC
- file bytes over TCP
- master-confirmed upload

---

### Heartbeat flow

1. Every keeper sends a heartbeat every second.
2. The master updates the node's `last_seen`.
3. A monitor loop checks if the heartbeat is too old.
4. If yes, the node is marked down.

This does not stop the whole DFS.
It only changes the master view of which replicas are safe to use.

---

### Replication flow

1. Every 10 seconds the master scans all tracked files.
2. For each file, it counts alive replicas.
3. If the file has fewer alive replicas than it should, the master picks a source and missing destinations.
4. The master tells the source keeper to copy the file.
5. The destination stores the file and reports back through `NotifyUpload`.
6. The master updates its index again.

If I only have 2 alive keepers, the system still works, but the dashboard shows the file as under-replicated instead of pretending that 2 out of 3 is enough.

---

### Download flow

1. The client asks the master which keepers currently have the file alive.
2. The master returns all alive locations.
3. If there is one replica, the client downloads the whole file from it.
4. If there are multiple replicas, the client splits the file into ranges and asks each keeper for one range.
5. After collecting all chunks, the client assembles the file in the right order.

This is why the data keepers support both:

- full download
- range download

---

## 4. How to run the project

### Option A: single-machine smoke test

From the project root:

```bash
chmod +x run_system.sh
./run_system.sh 3
```

Then open:

- Dashboard: `http://localhost:8080`
- Client UI: `http://localhost:8081`

This is the fastest way to verify:

- heartbeats
- upload
- replication
- download

---

### Option B: real 2-device Linux test

This is the setup I recommend for your current situation because you said you have 2 devices now.

The layout is:

- **Device A**: master, client, node1
- **Device B**: node2, node3

This still gives you **3 keepers total**, so you can meet the assignment's 3-replica target.

#### Before starting

Make sure both devices:

1. Are on the same LAN.
2. Can ping each other.
3. Have Go installed.
4. Allow these ports through the firewall:
   - `50051` for master gRPC
   - `8080` for dashboard
   - `8081` for client UI
   - `7001`, `7002`, `7003` for keeper TCP file transfer
   - `8001`, `8002`, `8003` for keeper gRPC control (`port + 1000`)

Replace the placeholders:

- `<DEVICE_A_IP>` with the LAN IP of Device A
- `<DEVICE_B_IP>` with the LAN IP of Device B

#### Device A commands

Start the master:

```bash
go run ./master/master_tracker.go -grpc-listen :50051 -dashboard-listen :8080
```

In a second terminal on Device A, start node1:

```bash
go run ./node/data_keeper.go -id node1 -port 7001 -master <DEVICE_A_IP>:50051 -advertise-ip <DEVICE_A_IP>
```

In a third terminal on Device A, start the client UI:

```bash
go run ./client/client.go -master <DEVICE_A_IP>:50051 -port 8081
```

#### Device B commands

In the first terminal on Device B, start node2:

```bash
go run ./node/data_keeper.go -id node2 -port 7002 -master <DEVICE_A_IP>:50051 -advertise-ip <DEVICE_B_IP>
```

In the second terminal on Device B, start node3:

```bash
go run ./node/data_keeper.go -id node3 -port 7003 -master <DEVICE_A_IP>:50051 -advertise-ip <DEVICE_B_IP>
```

#### What to open in the browser

Open these from Device A:

- Dashboard: `http://<DEVICE_A_IP>:8080`
- Client UI: `http://<DEVICE_A_IP>:8081`

#### What to test

1. Upload one `.mp4` from the client UI.
2. Watch the dashboard:
   - all 3 keepers should appear alive
   - the file should first appear with 1 replica
   - within the replication cycle it should reach 3 alive replicas
3. Download the same file and confirm it opens correctly.
4. Stop one keeper and refresh the dashboard.
5. You should see:
   - that node marked down
   - the file still readable if at least one alive replica remains
   - the file marked under-replicated until the missing keeper comes back

---

## 5. How I would explain the code in discussion

If someone asks me to explain the code naturally, I would say something like this:

> I designed the system around a central master because the assignment explicitly describes a master tracker with a lookup table. The master is responsible for placement, liveness tracking, and replication planning. The data keepers stay simpler: they store files, send heartbeats, and serve TCP transfers. The client acts as a control-plane user that first talks to the master, then talks directly to the chosen keeper for the actual file bytes.

Then I would go one layer deeper:

> In `dfs.proto` I separated responsibilities clearly. The master-facing RPCs are about metadata and coordination, while TCP is kept only for the file payload itself. In the master implementation I used a file index keyed by file name and node id because that makes counting replicas and checking holders very direct. In the keeper implementation I made the upload path notify the master after the file is actually written, which is why the client can now wait for a real master confirmation. In the client implementation I kept the parallel chunk download logic because the assignment says the client should request from every returned port uniformly.

And if they ask "what changed compared to the older version?", I would say:

> The biggest fixes were removing the fake file-listing hack, adding upload confirmation through the master, making replication schedule all missing copies in one pass, and making the dashboard show real replica health instead of cosmetic log messages.

---

## 6. Important behavior notes

- Only `.mp4` uploads are accepted.
- Duplicate file names are rejected to avoid conflicting copies with the same logical name.
- A file is considered **fully replicated** only when it has **3 alive replicas**.
- If fewer than 3 keepers are alive, the system still works as long as at least one alive replica exists, but the dashboard and file list will mark the file as under-replicated.
- Delete and wipe are optional extra operations for testing and demo cleanup.

---

## 7. Quick viva answers

### Why gRPC and TCP together?

Because the assignment explicitly asks for:

- gRPC for communication between components
- TCP for file transfer

So I used gRPC for coordination and TCP for the MP4 content itself.

### Why is the master central?

Because the assignment architecture is centralized and based on a master tracker with a lookup table.

### How do you know a node is alive?

From the keeper heartbeat every second plus the master's timeout-based liveness monitor.

### What happens if one keeper goes down?

The master marks it down, removes it from alive download choices, and the file becomes under-replicated until enough alive keepers are available again.

### How does replication happen?

The master runs a replication loop every 10 seconds, chooses a source holder and missing destinations, then tells the source keeper to copy the file to those destinations.

### Why did you add `WaitForUpload`?

So the client only declares success after the master actually records the file, which makes the upload flow reliable and matches the expected sequence better.
