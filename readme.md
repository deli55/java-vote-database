
---

# Java Vote Database (RaftKVStore)

A lightweight, zero-dependency, fault-tolerant distributed key-value store implemented in **Java 21**, powered by the **Raft Consensus Algorithm** and **Write-Ahead Logging (WAL)**.

`RaftKVStore` delivers linearizable state machine replication across a cluster of nodes using modern Java features—including **Virtual Threads (Project Loom)** and **Records**—over a custom length-prefixed TCP network protocol, relying solely on the standard JDK.

---

## Key Features

* **Complete Raft Consensus Implementation:**
* **Leader Election:** Randomized election timeouts (300 ms – 600 ms) with automatic term advancement, self-voting, and candidate step-down logic.
* **Heartbeat & Keep-Alive:** Periodic leader heartbeats (50 ms) to maintain leadership and prevent split-brain scenarios.
* **Log Replication & Quorum Commit:** Asynchronous parallel log distribution across followers, advancing commit index upon majority (N / 2 + 1) consensus.
* **Follower Catch-Up:** Automatic log reconciliation and truncation upon follower network partitioning or server restarts.


* **Pure Java 21 Engine:** Built with zero external runtime dependencies—utilizes `Executors.newVirtualThreadPerTaskExecutor()` for non-blocking I/O and Java `record` types for log entries and messages.
* **Durable Write-Ahead Logging (WAL):** Disk-backed append-only `wal.log` file with instant flush semantics for crash resilience and automatic state machine recovery on startup.
* **Custom Binary Framing Protocol:** Ultra-low overhead 4-byte big-endian length-prefixed socket messaging protocol over plain TCP sockets.
* **Thread-Safe State Machine:** Uses a fair `ReentrantReadWriteLock` wrapping a `ConcurrentHashMap` for high-throughput concurrent reads and writes.
* **Interactive CLI & Embedded Server:** Built-in command-line interface supporting direct `PUT`, `GET`, `DELETE`, and `NO_OP` operations.

---

## Architecture & System Topology

```text
                                  ┌──────────────────────────────┐
                                  │      Client / CLI Shell      │
                                  └──────────────┬───────────────┘
                                                 │
                                                 │ CLIENT_REQUEST (TCP)
                                                 ▼
 ┌─────────────────────────────────────────────────────────────────────────────────────────┐
 │ LEADER NODE                                                                             │
 │                                                                                         │
 │  ┌──────────────────┐     Log Append     ┌──────────────────────┐     Sync Flush    ┌──┴────────────┐
 │  │  RaftNodeServer  │ ─────────────────> │ KVStoreStateMachine  │ ────────────────> │  wal.log      │
 │  └────────┬─────────┘                    └──────────────────────┘                   └───────────────┘
 └───────────┼─────────────────────────────────────────────────────────────────────────────┘
             │
             │ APPEND_ENTRIES (Virtual Threads)
             ├─────────────────────────────────────────┐
             ▼                                         ▼
 ┌─────────────────────────┐               ┌─────────────────────────┐
 │ FOLLOWER NODE 1         │               │ FOLLOWER NODE 2         │
 │                         │               │                         │
 │  ┌───────────────────┐  │               │  ┌───────────────────┐  │
 │  │  RaftNodeServer   │  │               │  │  RaftNodeServer   │  │
 │  └────────┬──────────┘  │               │  └────────┬──────────┘  │
 │           ▼             │               │           ▼             │
 │  ┌───────────────────┐  │               │  ┌───────────────────┐  │
 │  │ wal.log (Disk)    │  │               │  │ wal.log (Disk)    │  │
 │  └───────────────────┘  │               │  └───────────────────┘  │
 └─────────────────────────┘               └─────────────────────────┘

```

---

## Network Protocol Specification

Communication between client-to-node and node-to-node utilizes a custom 4-byte big-endian length-prefixed UTF-8 encoded binary frame:

```text
+-------------------+-------------------------------------+
| LENGTH (4 bytes)  | PAYLOAD (UTF-8 Encoded String)      |
| Big-Endian Integer| Max: 1,048,576 bytes (1 MB)         |
+-------------------+-------------------------------------+

```

### Supported Message Types (`MessageType`)

* `CLIENT_REQUEST` / `CLIENT_RESPONSE`: Inter-node client reads and writes.
* `VOTE_REQUEST` / `VOTE_RESPONSE`: Candidate leader election votes.
* `APPEND_ENTRIES` / `APPEND_ENTRIES_RESPONSE`: Log replication payloads and leader heartbeats.
* `HEARTBEAT`: Periodic cluster health verification.

---

## Package Layout

```text
com.raftkv/
├── RaftKVStore.java            # Main entry point & interactive CLI interface
├── RaftNodeServer.java         # Raft consensus engine, peer management & RPC server
├── KVStoreStateMachine.java    # Thread-safe Key-Value store with disk-backed WAL
├── LogEntry.java               # Immutable record for log entries
├── NodeState.java              # Enum: FOLLOWER, CANDIDATE, LEADER
└── protocol/
    ├── CommandType.java        # Enum: PUT, GET, DELETE, NO_OP
    ├── MessageType.java        # Enum: CLIENT_REQUEST, APPEND_ENTRIES, VOTE_REQUEST, etc.
    ├── Message.java            # Network message serialization record
    └── ProtocolCodec.java      # Socket byte encoder / decoder

```

---

## Prerequisites

* **Java Development Kit (JDK):** Version 21 or higher.
* **Build Tool:** Gradle 8.7+ (wrapper included).

---

## Building the Project

The project uses the Gradle Shadow Plugin to produce a single self-contained executable Fat JAR.

```bash
# Clone the repository
git clone https://github.com/your-username/java-vote-database.git
cd java-vote-database

# Build executable shadow JAR
./gradlew shadowJar

```

The compiled artifact will be located at:

```text
build/libs/java-vote-database-1.0.0.jar

```

---

## Usage Guide

### 1. Launching a Standalone Node

Start a server on default port `9000` with data stored in `./data`:

```bash
java -jar build/libs/java-vote-database-1.0.0.jar

```

Start a node on a custom port and data directory:

```bash
java -jar build/libs/java-vote-database-1.0.0.jar 9001 /var/lib/raft-data

```

### 2. Interactive CLI Commands

Once launched, you can issue commands directly through the interactive console shell:

```text
RaftKVStore> PUT user:1001 {"name":"Alice","role":"admin"}
SUCCESS

RaftKVStore> GET user:1001
user:1001 = {"name":"Alice","role":"admin"}

RaftKVStore> DELETE user:1001
SUCCESS

RaftKVStore> quit
Exiting RaftKVStore.

```

---

## Running the Unit & Integration Test Suite

The test suite includes multi-node integration tests simulating 3-node clusters, leader elections, network partitioning, and log catch-up:

```bash
# Run all unit and integration tests
./gradlew test

```

### Key Test Coverage

* `RaftNodeServerTest`: Single-node operation, WAL recovery, concurrent reads/writes.
* `RaftLeaderElectionTest`: Multi-node cluster elections, term increments, heartbeat exchanges.
* `RaftLogReplicationTest`: Distributed log replication, quorum commits, follower log truncation and recovery.

---

## Configuration & Tuning Parameters

Key protocol settings are defined in `RaftNodeServer.java`:

| Parameter | Default Value | Description |
| --- | --- | --- |
| `ELECTION_TIMEOUT_MIN_MS` | `300 ms` | Minimum randomized election timeout |
| `ELECTION_TIMEOUT_MAX_MS` | `600 ms` | Maximum randomized election timeout |
| `HEARTBEAT_INTERVAL_MS` | `50 ms` | Leader heartbeat push frequency |
| `MAX_MESSAGE_SIZE` | `1,048,576` (1 MB) | Maximum socket frame size |
| `CLIENT_SOCKET_TIMEOUT_MS` | `30,000 ms` | Socket read timeout |

---

## License

Distributed under the MIT License. See `LICENSE` for details.