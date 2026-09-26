# Java 21 Distributed Raft Key-Value Store

A lightweight, fault-tolerant distributed key-value database built from scratch in Java 21 using custom TCP networking and the Raft consensus algorithm.

---

## What Is This Project?

Imagine running a database on a single computer. If that computer crashes, your entire application goes down and you might lose data. 

To solve this, this project connects multiple computers (nodes) into a cluster that acts like a single database. As long as a majority of the nodes are running (for example, 2 out of 3), your data remains completely safe, consistent, and available—even if one computer suddenly crashes or gets disconnected.

---

## How It Works (In Plain English)

1. **Leader Election (The "Boss" Node)**
   - When the cluster starts, the nodes hold an automatic vote.
   - One node is elected as the Leader, while the others become Followers.
   - All database updates (PUT / DELETE) go through the Leader.
   - If the Leader crashes, the Followers detect the silence within ~300ms and automatically elect a new Leader.

2. **Majority Consensus (2 out of 3 Agreement)**
   - When you write data, the Leader sends a copy to all Followers.
   - The Leader only confirms the write as saved (committed) once a majority (at least 2 out of 3 nodes) confirms they received it.
   - This prevents data loss if a single node suddenly disconnects.

3. **Write-Ahead Log (WAL) & Crash Recovery**
   - Every operation is written to a disk file (Write-Ahead Log) before it is processed in memory.
   - If a node loses power or restarts, it reads its log file on boot and restores its state instantly.

4. **Follower Catch-Up**
   - If a Follower goes offline while writes are happening, it will fall behind.
   - When it comes back online, the Leader detects what it missed and automatically sends the missing history until the Follower is caught up.

5. **Modern Java 21 Infrastructure**
   - Uses Java 21 Virtual Threads to handle concurrent TCP socket connections across nodes efficiently with minimal memory overhead.

---

## Project Structure

src/
├── main/java/com/raftkv/
│   ├── RaftNodeServer.java        # Core node server, election state, & consensus rules
│   ├── KVStoreStateMachine.java   # In-memory key-value store & Write-Ahead Log (WAL)
│   ├── LogEntry.java              # Log entry model (term, index, operation)
│   ├── Message.java               # Network RPC protocols (Votes, Heartbeats, Append Entries)
│   └── ProtocolCodec.java         # TCP socket message serialization/deserialization
└── test/java/com/raftkv/
├── RaftNodeServerTest.java     # Single-node & WAL crash recovery unit tests
├── RaftLeaderElectionTest.java # 3-node cluster leader election integration tests
└── RaftLogReplicationTest.java # Multi-node log replication & catch-up integration tests

---

## How to Run & Test

### Prerequisites
- Java 21 JDK or newer installed.

### 1. Run All Tests
To verify single-node storage, cluster leader election, and distributed replication:

**Windows (PowerShell/CMD):**
```powershell
.\gradlew.bat test

**Linux / macOS:**
Bash
./gradlew test

### 2. Run Specific Test Suites

If you want to test a specific layer of the system:
PowerShell

# Run only leader election tests
.\gradlew.bat test --tests "RaftLeaderElectionTest"

# Run log replication & catch-up tests
.\gradlew.bat test --tests "RaftLogReplicationTest"

### 3. Build the Project

To compile and package the project:
PowerShell

.\gradlew.bat build