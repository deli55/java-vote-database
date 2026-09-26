package com.raftkv;

import com.raftkv.protocol.*;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Raft node server implementing leader election, heartbeat, and log replication.
 *
 * <p>Each node maintains Raft state (term, votedFor, log, commitIndex and
 * communicates with peers via TCP using Java 21 Virtual Threads.
 */
public class RaftNodeServer {

    private final int port;
    private final String nodeId;
    private final KVStoreStateMachine kvStore;
    private final NodeState nodeState;
    private final AtomicReference<NodeState> stateRef;

    // Raft state (mutable, thread-safe)
    private final AtomicInteger currentTerm = new AtomicInteger(0);
    private final AtomicReference<String> votedFor = new AtomicReference<>(null);
    private final AtomicInteger commitIndex = new AtomicInteger(0);
    private final AtomicInteger lastApplied = new AtomicInteger(0);

    // Peer configuration: nodeId -> host:port
    private volatile Map<String, String> peers = Map.of();

    // Leader state: per-peer tracking for log replication
    private volatile Map<String, Integer> nextIndex;
    private volatile Map<String, Integer> matchIndex;

    // Leader election state
    private volatile AtomicReference<NodeState> currentState;
    private final Object timerLock = new Object();
    private volatile ScheduledExecutorService electionTimer;
    private volatile ScheduledExecutorService heartbeatExecutor;
    private volatile ScheduledFuture<?> electionTimeoutFuture;
    private volatile ScheduledFuture<?> heartbeatFuture;
    private volatile ConcurrentHashMap<String, AtomicInteger> votesReceived;
    private volatile String currentLeader = null;
    private volatile boolean electionInProgress = false;
    // Track which candidates we've voted for (to allow voting for all at same term)
    private final ConcurrentHashMap<String, Boolean> votesCast = new ConcurrentHashMap<>();

    // Networking
    private ServerSocket serverSocket;
    private ExecutorService executor;
    private final AtomicBoolean running = new AtomicBoolean(false);

    private static final int ELECTION_TIMEOUT_MIN_MS = 300;
    private static final int ELECTION_TIMEOUT_MAX_MS = 600;
    private static final int HEARTBEAT_INTERVAL_MS = 50;
    private static final int PEER_CONNECT_TIMEOUT_MS = 1000;

    /**
     * Create a new RaftNodeServer.
     *
     * @param port      the port to listen on
     * @param kvStore   the key-value store state machine
     * @param nodeId    the unique node ID
     * @param nodeState the initial node state
     * @param currentTerm the initial term
     */
    public RaftNodeServer(int port, KVStoreStateMachine kvStore, String nodeId, NodeState nodeState, int currentTerm) {
        this.port = port;
        this.nodeId = nodeId != null ? nodeId : UUID.randomUUID().toString();
        this.kvStore = kvStore;
        this.nodeState = nodeState != null ? nodeState : NodeState.FOLLOWER;
        this.stateRef = new AtomicReference<>(nodeState);
        this.currentState = this.stateRef;
        this.currentTerm.set(currentTerm);
        this.votesReceived = new ConcurrentHashMap<>();
    }

    /**
     * Set the peer map (nodeId -> host:port).
     */
    public void setPeers(Map<String, String> peers) {
        this.peers = peers != null ? Map.copyOf(peers) : Map.of();
        // Initialize leader state if we have peers
        if (peers != null && !peers.isEmpty()) {
            this.nextIndex = new ConcurrentHashMap<>();
            this.matchIndex = new ConcurrentHashMap<>();
            for (String peerId : peers.keySet()) {
                this.nextIndex.put(peerId, 0);
                this.matchIndex.put(peerId, 0);
            }
            // Include ourselves
            this.nextIndex.put(this.nodeId, 0);
            this.matchIndex.put(this.nodeId, 0);
        }
    }

    /**
     * Get the current node state.
     */
    public NodeState getNodeState() {
        return currentState.get();
    }

    /**
     * Get the current term.
     */
    public int getCurrentTerm() {
        return currentTerm.get();
    }

    /**
     * Get the current leader.
     */
    public String getLeaderId() {
        return currentLeader;
    }

    /**
     * Get the commit index.
     */
    public int getCommitIndex() {
        return commitIndex.get();
    }

    /**
     * Get the node ID.
     */
    public String getNodeId() {
        return nodeId;
    }

    /**
     * Get the port.
     */
    public int getPort() {
        return port;
    }

    /**
     * Get the KV store (for testing).
     */
    public KVStoreStateMachine getKvStore() {
        return kvStore;
    }

    /**
     * Get the peer map.
     */
    public Map<String, String> getPeers() {
        return peers;
    }

    /**
     * Start the server and begin listening for connections.
     */
    public void start() throws IOException {
        if (running.get()) {
            throw new IOException("Server is already running");
        }

        executor = Executors.newVirtualThreadPerTaskExecutor();
        serverSocket = new ServerSocket();
        serverSocket.bind(new InetSocketAddress(port));
        running.set(true);

        // If starting as FOLLOWER, schedule election timer
        if (currentState.get() == NodeState.FOLLOWER || currentState.get() == NodeState.CANDIDATE) {
            scheduleElectionTimer();
        }

        while (running.get()) {
            try {
                Socket clientSocket = serverSocket.accept();
                executor.submit(() -> handleClient(clientSocket));
            } catch (IOException e) {
                if (running.get()) {
                    System.err.println("Error accepting connection: " + e.getMessage());
                }
            }
        }
    }

    /**
     * Stop the server.
     */
    public void stop() throws InterruptedException {
        running.set(false);

        // Cancel all timers
        cancelElectionTimer();
        cancelHeartbeat();

        if (serverSocket != null && !serverSocket.isClosed()) {
            try {
                serverSocket.close();
            } catch (IOException e) {
                // Ignore during shutdown
            }
        }

        if (executor != null) {
            executor.shutdown();
            if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
                executor.shutdownNow();
            }
        }
    }

    /**
     * Check if the server is running.
     */
    public boolean isRunning() {
        return running.get();
    }

    /**
     * Handle a client connection.
     */
    private void handleClient(Socket clientSocket) {
        try (clientSocket) {
            clientSocket.setSoTimeout(30000);

            while (!clientSocket.isClosed() && running.get()) {
                try {
                    Message request = ProtocolCodec.decode(clientSocket.getInputStream());
                    Message response = processRequest(request);
                    // Only send response if not null (e.g., VOTE_RESPONSE is fire-and-forget)
                    if (response != null) {
                        ProtocolCodec.encode(response, clientSocket.getOutputStream());
                    }
                } catch (IOException e) {
                    break;
                }
            }
        } catch (IOException e) {
            System.err.println("Error handling client: " + e.getMessage());
        }
    }

    /**
     * Process an incoming request (both client and peer messages).
     */
    private Message processRequest(Message request) {
        try {
            switch (request.type()) {
                case VOTE_REQUEST -> {
                    return handleVoteRequest(request);
                }
                case VOTE_RESPONSE -> {
                    handleVoteResponse(request);
                    return null;
                }
                case HEARTBEAT -> {
                    return handleHeartbeat(request);
                }
                case APPEND_ENTRIES -> {
                    return handleAppendEntries(request);
                }
                case APPEND_ENTRIES_RESPONSE -> {
                    handleAppendEntriesResponse(request);
                    return null;
                }
                case CLIENT_REQUEST -> {
                    return handleClientRequest(request);
                }
                default -> {
                    return Message.clientResponse(
                            null, null, request.requestId(),
                            "FAILURE", "Unknown message type: " + request.type(), nodeId, currentTerm.get()
                    );
                }
            }
        } catch (Exception e) {
            return Message.clientResponse(
                    request.key(), null, request.requestId(),
                    "FAILURE", e.getMessage(), nodeId, currentTerm.get()
            );
        }
    }

    // ==================== VOTING ====================

    /**
     * Handle an incoming VOTE_REQUEST.
     * Grant vote if term >= currentTerm and (votedFor == null or votedFor == candidateId).
     */
    private Message handleVoteRequest(Message request) {
        int candidateIdTerm = request.term();
        String candidateId = request.serverId();

        // Update term and step down if candidate has higher term
        if (candidateIdTerm > currentTerm.get()) {
            currentTerm.set(candidateIdTerm);
            votedFor.set(null);
            setState(NodeState.FOLLOWER);
            cancelElectionTimer();
            cancelHeartbeat();
            scheduleElectionTimer();
        }

        // Only grant vote if candidate's term >= our term AND candidate's log is at least as up-to-date
        boolean voteGranted = false;
        if (candidateIdTerm >= currentTerm.get()) {
            boolean logUpToDate = true;
            if (!kvStore.getLog().isEmpty()) {
                LogEntry lastEntry = kvStore.getLog().get(kvStore.getLog().size() - 1);
                if (request.prevLogTerm() < lastEntry.term()) {
                    logUpToDate = false;
                }
            }

            if (logUpToDate) {
                // Grant vote to all candidates at same/higher term (except ourselves)
                if (!candidateId.equals(nodeId)) {
                    voteGranted = true;
                    // Track that we've voted for this candidate
                    votesCast.put(candidateId, Boolean.TRUE);
                } else {
                    // We're receiving a vote request from ourselves (in onElectionTimeout we set votedFor = nodeId)
                    voteGranted = true;
                    votesCast.put(nodeId, Boolean.TRUE);
                }
                // Reset election timer
                cancelElectionTimer();
                scheduleElectionTimer();
            }
        }

        Message response = Message.voteResponse(nodeId, currentTerm.get(), voteGranted);
        return response;
    }

    /**
     * Handle an incoming VOTE_RESPONSE.
     * Aggregate votes; when majority reached, transition to LEADER.
     */
    private void handleVoteResponse(Message response) {
        if (currentState.get() != NodeState.CANDIDATE) {
            return; // Already stepped down
        }

        int term = response.term();
        boolean voteGranted = response.voteGranted();
        String voterId = response.serverId();

        // Only accept responses for current term
        if (term != currentTerm.get()) {
            return;
        }

        if (!voteGranted) {
            return;
        }

        // Only count votes from peers (not ourselves)
        if (voterId.equals(nodeId)) {
            return;
        }

        // Count vote from this peer (each peer can only vote once per term)
        votesReceived.computeIfAbsent(voterId, k -> new AtomicInteger(0)).incrementAndGet();

        // Count total votes: sum of all votes in votesReceived
        // (each node starts with 1 vote for itself from onElectionTimeout,
        // and each peer's vote is added when we receive their VOTE_RESPONSE)
        int totalVotes = votesReceived.values().stream().mapToInt(AtomicInteger::get).sum();

        // Check for majority
        int majority = (peers.size() / 2) + 1;
        if (totalVotes >= majority) {
            becomeLeader();
        }
    }

    /**
     * Transition to LEADER state.
     */
    private void becomeLeader() {
        setState(NodeState.LEADER);
        currentLeader = nodeId;
        cancelElectionTimer();
        votesReceived.clear();
        electionInProgress = false;

        // Initialize leader state for each peer
        int lastLogIndex = kvStore.getLog().size();
        for (String peerId : peers.keySet()) {
            nextIndex.put(peerId, lastLogIndex + 1);
            matchIndex.put(peerId, 0);
        }
        // Include ourselves
        nextIndex.put(nodeId, lastLogIndex + 1);
        matchIndex.put(nodeId, kvStore.getLog().size());

        // Create a NO_OP entry for the current term if this is a new term
        // (standard Raft practice: leader must commit a NO_OP for each uncommitted term)
        try {
            if (!kvStore.getLog().isEmpty()) {
                LogEntry lastEntry = kvStore.getLog().get(kvStore.getLog().size() - 1);
                if (lastEntry.term() != currentTerm.get()) {
                    LogEntry noOp = LogEntry.fromCommand(currentTerm.get(), CommandType.NO_OP, null, null);
                    kvStore.applyLogEntry(noOp);
                    // Update matchIndex for self after adding NO_OP
                    matchIndex.put(nodeId, kvStore.getLog().size());
                }
            } else {
                // Empty log - create NO_OP entry
                LogEntry noOp = LogEntry.fromCommand(currentTerm.get(), CommandType.NO_OP, null, null);
                kvStore.applyLogEntry(noOp);
                matchIndex.put(nodeId, kvStore.getLog().size());
            }
            // Update nextIndex for all peers to start from the first entry (index 1)
            // This ensures all entries including the NO_OP are sent to followers
            lastLogIndex = kvStore.getLog().size();
            for (String peerId : peers.keySet()) {
                nextIndex.put(peerId, 1);
            }
            nextIndex.put(nodeId, 1);
        } catch (Exception e) {
            System.err.println("Failed to create NO_OP entry: " + e.getMessage());
        }

        // Start heartbeat loop
        startHeartbeat();

        // Send initial heartbeat immediately
        sendHeartbeatToAll();
    }

    /**
     * Transition to FOLLOWER state.
     */
    private void becomeFollower(int term, String leaderId) {
        setState(NodeState.FOLLOWER);
        currentTerm.set(term);
        votedFor.set(null);
        currentLeader = leaderId;
        cancelElectionTimer();
        cancelHeartbeat();
        votesReceived.clear();
        electionInProgress = false;
        scheduleElectionTimer();
    }

    // ==================== HEARTBEAT ====================

    /**
     * Handle an incoming HEARTBEAT from the leader.
     */
    private Message handleHeartbeat(Message request) {
        int leaderTerm = request.term();

        // Update term if needed
        if (leaderTerm >= currentTerm.get()) {
            if (leaderTerm > currentTerm.get()) {
                currentTerm.set(leaderTerm);
            }

            // Step down if we were leader or candidate
            if (currentState.get() != NodeState.FOLLOWER) {
                setState(NodeState.FOLLOWER);
                cancelHeartbeat();
            }

            votedFor.set(null);
            currentLeader = request.serverId();

            // Reset election timer
            cancelElectionTimer();
            scheduleElectionTimer();

            // Update commitIndex monotonically
            int leaderCommit = request.commitIndex();
            int localCommitIndex = Math.min(leaderCommit, kvStore.getLog().size());
            int currentCommitIndex = commitIndex.get();
            if (localCommitIndex > currentCommitIndex) {
                commitIndex.set(localCommitIndex);
                kvStore.updateCommitIndex(localCommitIndex);
                kvStore.applyCommittedEntries();
            }
        }

        return Message.clientResponse(
                null, null, null,
                "SUCCESS", null, nodeId, currentTerm.get()
        );
    }

    /**
     * Handle APPEND_ENTRIES (used for replication).
     * Followers append new entries to kvStore BEFORE constructing response.
     */
    private Message handleAppendEntries(Message request) {
        int leaderTerm = request.term();

        if (leaderTerm >= currentTerm.get()) {
            if (leaderTerm > currentTerm.get()) {
                currentTerm.set(leaderTerm);
            }
            setState(NodeState.FOLLOWER);
            votedFor.set(null);
            currentLeader = request.serverId();
            cancelElectionTimer();
            scheduleElectionTimer();

            // Update commitIndex
            int leaderCommit = request.commitIndex();
            int localCommitIndex = Math.min(leaderCommit, kvStore.getLog().size());
            int currentCommitIndex = commitIndex.get();
            if (localCommitIndex > currentCommitIndex) {
                commitIndex.set(localCommitIndex);
                kvStore.updateCommitIndex(localCommitIndex);
                kvStore.applyCommittedEntries();
            }
        }

        int prevLogIndex = request.prevLogIndex();
        int prevLogTerm = request.prevLogTerm();
        String[] entries = request.entries();

        // Verify prevLogIndex and prevLogTerm
        if (entries != null && entries.length > 0) {
            if (prevLogIndex > 0) {
                List<LogEntry> log = kvStore.getLog();
                if (prevLogIndex > log.size()) {
                    // Log mismatch - return failure with current log size as matchIndex
                    return Message.appendEntriesResponse(nodeId, currentTerm.get(), false, kvStore.getLog().size());
                }
                // Check term of the entry at prevLogIndex (1-based index, so subtract 1)
                LogEntry prevEntry = log.get(prevLogIndex - 1);
                if (prevEntry.term() != prevLogTerm) {
                    // Term mismatch - return current log size
                    return Message.appendEntriesResponse(nodeId, currentTerm.get(), false, kvStore.getLog().size());
                }
            }

            // Append new entries to the log BEFORE constructing response
            List<LogEntry> log = kvStore.getLog();

            for (int i = 0; i < entries.length; i++) {
                String entryStr = entries[i];
                if (entryStr.isBlank()) continue;

                int entryIndex = prevLogIndex + i + 1; // 1-based index

                // Check if this entry is already in the log
                if (entryIndex <= log.size()) {
                    // Entry at this index already exists, check if it matches
                    LogEntry existingEntry = log.get(entryIndex - 1);
                    if (existingEntry.serialize().equals(entryStr)) {
                        continue; // Entry already exists, skip it
                    }
                    // If the entry doesn't match, we need to replace it
                    LogEntry newEntry = parseLogEntry(entryStr, entryIndex);
                    if (newEntry != null) {
                        log.set(entryIndex - 1, newEntry);
                    }
                } else {
                    // Add new entry with the correct index
                    LogEntry newEntry = parseLogEntry(entryStr, entryIndex);
                    if (newEntry != null) {
                        try {
                            kvStore.appendEntryWithIndex(newEntry);
                        } catch (Exception e) {
                            // Skip entries that fail to apply
                        }
                    }
                }
            }
        }

        // Truncate any entries beyond what the leader has
        // The last entry the leader sent is at index: prevLogIndex + entries.length
        int leaderLastEntryIndex = prevLogIndex + (entries != null ? entries.length : 0);
        List<LogEntry> log = kvStore.getLog();
        if (leaderLastEntryIndex < log.size()) {
            // Remove extra entries from the end of the log
            while (log.size() > leaderLastEntryIndex) {
                log.remove(log.size() - 1);
            }
        }

        // Return success with matchIndex = current log size
        return Message.appendEntriesResponse(nodeId, currentTerm.get(), true, kvStore.getLog().size());
    }

    /**
     * Parse a log entry from a serialized string.
     * The index is extracted from the serialized string itself.
     */
    private LogEntry parseLogEntry(String entryStr, int defaultIndex) {
        try {
            // Parse: LOG{index=...,term=...,commandType=...,key=...,value=...}
            String content = entryStr;
            if (content.startsWith("LOG{")) {
                content = content.substring(4, content.length() - 1);
            }

            String commandType = null;
            String key = null;
            String value = null;
            int term = 0;
            int index = defaultIndex;

            int i = 0;
            while (i < content.length()) {
                // Skip spaces and commas between fields
                while (i < content.length() && (content.charAt(i) == ' ' || content.charAt(i) == ',')) i++;
                int eqIndex = content.indexOf('=', i);
                if (eqIndex == -1) break;

                String field = content.substring(i, eqIndex).trim();
                i = eqIndex + 1;
                while (i < content.length() && content.charAt(i) == ' ') i++;

                if (i >= content.length()) break;

                char c = content.charAt(i);
                switch (field) {
                    case "index" -> {
                        int end = i;
                        while (end < content.length() && Character.isDigit(content.charAt(end))) end++;
                        index = Integer.parseInt(content.substring(i, end));
                        i = end;
                    }
                    case "term" -> {
                        int end = i;
                        while (end < content.length() && Character.isDigit(content.charAt(end))) end++;
                        term = Integer.parseInt(content.substring(i, end));
                        i = end;
                    }
                    case "commandType" -> {
                        if (c == '"') {
                            int end = findStringEnd(content, i + 1);
                            commandType = content.substring(i + 1, end);
                            i = end + 1;
                        } else {
                            int end = i;
                            while (end < content.length() && content.charAt(end) != ',' && content.charAt(end) != '}') end++;
                            commandType = content.substring(i, end).trim();
                            i = end;
                        }
                    }
                    case "key" -> {
                        if (c == '"') {
                            int end = findStringEnd(content, i + 1);
                            key = content.substring(i + 1, end);
                            i = end + 1;
                        } else {
                            int end = i;
                            while (end < content.length() && content.charAt(end) != ',' && content.charAt(end) != '}') end++;
                            key = content.substring(i, end).trim();
                            i = end;
                        }
                    }
                    case "value" -> {
                        if (c == '"') {
                            int end = findStringEnd(content, i + 1);
                            value = content.substring(i + 1, end);
                            i = end + 1;
                        } else {
                            int end = i;
                            while (end < content.length() && content.charAt(end) != ',' && content.charAt(end) != '}') end++;
                            value = content.substring(i, end).trim();
                            i = end;
                        }
                    }
                    default -> {
                        i++;
                    }
                }
            }

            if (commandType != null) {
                return LogEntry.fromCommand(index, term, CommandType.fromString(commandType), key, value);
            }
            return null;
        } catch (Exception e) {
            return null;
        }
    }

    private int findStringEnd(String content, int start) {
        for (int i = start; i < content.length(); i++) {
            if (content.charAt(i) == '"') return i;
        }
        return content.length();
    }

    /**
     * Handle a CLIENT_REQUEST.
     */
    private Message handleClientRequest(Message request) {
        if (currentState.get() != NodeState.LEADER) {
            return Message.clientResponse(
                    request.key(), null, request.requestId(),
                    "FAILURE", "Not leader", nodeId, currentTerm.get()
            );
        }

        CommandType commandType = request.commandType();
        if (commandType == null) {
            return Message.clientResponse(
                    null, null, request.requestId(),
                    "FAILURE", "Missing command type", nodeId, currentTerm.get()
            );
        }

        LogEntry entry = LogEntry.fromCommand(
                currentTerm.get(),
                commandType,
                request.key(),
                request.value()
        );

        String result;
        try {
            result = kvStore.applyLogEntry(entry);
        } catch (Exception e) {
            return Message.clientResponse(
                    request.key(), null, request.requestId(),
                    "FAILURE", e.getMessage(), nodeId, currentTerm.get()
            );
        }

        // Update leader's own matchIndex (self-replication counts toward majority)
        // The leader has replicated the entry to itself, so update matchIndex for self.
        // This is essential for quorum counting: self + peers with matchIndex >= N must reach majority.
        if (matchIndex != null) {
            matchIndex.put(nodeId, kvStore.getLog().size());
        }

        // Replicate to followers asynchronously
        replicateToFollowers();

        // Advance commit index based on strict majority quorum.
        // Quorum = (totalNodes / 2) + 1 where totalNodes = peers.size() + 1 (including self).
        // An entry at index N is committed when: count of nodes with matchIndex >= N >= quorum.
        // Self is always counted (matchIndex for self = log.size()).
        advanceCommitIndex();

        switch (commandType) {
            case PUT -> {
                return Message.clientResponse(
                        request.key(), request.value(), request.requestId(),
                        "SUCCESS", null, nodeId, currentTerm.get()
                );
            }
            case GET -> {
                return Message.clientResponse(
                        request.key(), result != null ? result : "", request.requestId(),
                        "SUCCESS", null, nodeId, currentTerm.get()
                );
            }
            case DELETE -> {
                return Message.clientResponse(
                        request.key(), null, request.requestId(),
                        "SUCCESS", null, nodeId, currentTerm.get()
                );
            }
            case NO_OP -> {
                return Message.clientResponse(
                        null, null, request.requestId(),
                        "SUCCESS", null, nodeId, currentTerm.get()
                );
            }
            default -> {
                return Message.clientResponse(
                        null, null, request.requestId(),
                        "FAILURE", "Unknown command: " + commandType, nodeId, currentTerm.get()
                );
            }
        }
    }

    // ==================== LOG REPLICATION ====================

    /**
     * Replicate the leader's log entries to all followers.
     * This is called after appending a new entry and periodically via heartbeats.
     */
    private void replicateToFollowers() {
        if (currentState.get() != NodeState.LEADER) {
            return;
        }

        for (Map.Entry<String, String> entry : peers.entrySet()) {
            String peerId = entry.getKey();
            if (peerId.equals(nodeId)) continue;

            Integer ni = nextIndex.get(peerId);
            if (ni == null || ni <= 0) continue;

            // Build the list of entries to send
            List<LogEntry> log = kvStore.getLog();
            int lastLogIndex = log.size();
            List<LogEntry> entriesToSend = new ArrayList<>();

            // Send entries starting from nextIndex - 1 (0-based index)
            for (int i = ni - 1; i < lastLogIndex; i++) {
                entriesToSend.add(log.get(i));
            }

            // Serialize entries
            String[] serializedEntries = new String[entriesToSend.size()];
            for (int i = 0; i < entriesToSend.size(); i++) {
                serializedEntries[i] = entriesToSend.get(i).serialize();
            }

            // Determine prevLogIndex and prevLogTerm
            int prevLogIndex = ni - 1; // 1-based index
            int prevLogTerm = 0;
            if (prevLogIndex > 0) {
                prevLogTerm = log.get(prevLogIndex - 1).term();
            }

            int leaderCommit = commitIndex.get();
            Message appendEntries = Message.appendEntries(
                    nodeId, currentTerm.get(), prevLogIndex, prevLogTerm,
                    serializedEntries, leaderCommit
            );

            String peerAddress = entry.getValue();
            sendReplicationMessage(peerId, peerAddress, appendEntries);
        }
    }

    /**
     * Send a replication message to a peer and handle the response.
     */
    private void sendReplicationMessage(String peerId, String peerAddress, Message message) {
        executor.submit(() -> {
            try (Socket socket = new Socket()) {
                socket.setSoTimeout(PEER_CONNECT_TIMEOUT_MS);
                // Parse host:port
                String[] parts = peerAddress.split(":", 2);
                if (parts.length != 2) return;
                String host = parts[0];
                int port = Integer.parseInt(parts[1]);
                socket.connect(new InetSocketAddress(host, port), PEER_CONNECT_TIMEOUT_MS);
                ProtocolCodec.encode(message, socket.getOutputStream());
                // Read the response
                Message response = ProtocolCodec.decode(socket.getInputStream());
                // Process the response
                if (response.type() == MessageType.APPEND_ENTRIES_RESPONSE) {
                    handleAppendEntriesResponse(response);
                }
            } catch (IOException e) {
                // Silently ignore failed RPCs
            }
        });
    }

    /**
     * Handle APPEND_ENTRIES_RESPONSE from a follower.
     * Updates matchIndex and nextIndex monotonically.
     */
    private void handleAppendEntriesResponse(Message response) {
        String followerId = response.serverId();
        int term = response.term();

        // Only accept responses from current term
        if (term != currentTerm.get()) {
            return;
        }

        if (currentState.get() != NodeState.LEADER) {
            return;
        }

        if (response.status() != null && response.status().equals("SUCCESS")) {
            // Success: update matchIndex and nextIndex
            int newMatchIndex = response.matchIndex();
            Integer currentMatch = matchIndex.getOrDefault(followerId, 0);
            if (newMatchIndex > currentMatch) {
                matchIndex.put(followerId, newMatchIndex);
            }
            // nextIndex = matchIndex + 1
            nextIndex.put(followerId, newMatchIndex + 1);

            // Try to advance commit index after each successful replication
            advanceCommitIndex();
        } else {
            // Failure (log mismatch): decrement nextIndex and retry
            Integer currentNext = nextIndex.getOrDefault(followerId, 1);
            int newNextIndex = Math.max(1, currentNext - 1);
            nextIndex.put(followerId, newNextIndex);

            // Retry with the new nextIndex
            String peerAddress = peers.get(followerId);
            if (peerAddress != null) {
                // Re-send with the decremented nextIndex
                List<LogEntry> log = kvStore.getLog();
                int lastLogIndex = log.size();
                List<LogEntry> entriesToSend = new ArrayList<>();

                for (int i = newNextIndex - 1; i < lastLogIndex; i++) {
                    entriesToSend.add(log.get(i));
                }

                String[] serializedEntries = new String[entriesToSend.size()];
                for (int i = 0; i < entriesToSend.size(); i++) {
                    serializedEntries[i] = entriesToSend.get(i).serialize();
                }

                int prevLogIndex = newNextIndex - 1;
                int prevLogTerm = 0;
                if (prevLogIndex > 0) {
                    prevLogTerm = log.get(prevLogIndex - 1).term();
                }

                int leaderCommit = commitIndex.get();
                Message retryEntries = Message.appendEntries(
                        nodeId, currentTerm.get(), prevLogIndex, prevLogTerm,
                        serializedEntries, leaderCommit
                );

                sendReplicationMessage(followerId, peerAddress, retryEntries);
            }
        }
    }

    /**
     * Advance the commit index by finding the highest index N > commitIndex
     * where a majority of peers have matchIndex >= N and log[N].term == currentTerm.
     */
    private void advanceCommitIndex() {
        if (currentState.get() != NodeState.LEADER) {
            return;
        }

        // If no matchIndex is initialized (single-node), commit everything
        if (matchIndex == null || matchIndex.isEmpty()) {
            int currentCommit = commitIndex.get();
            int logSize = kvStore.getLog().size();
            if (logSize > currentCommit) {
                commitIndex.set(logSize);
                kvStore.updateCommitIndex(logSize);
                kvStore.applyCommittedEntries();
            }
            return;
        }

        int currentCommit = commitIndex.get();
        List<LogEntry> log = kvStore.getLog();
        int majority = (peers.size() / 2) + 1;

        // Search for the highest index N > commitIndex that a majority has replicated
        for (int n = Math.max(currentCommit + 1, log.size()); n > currentCommit; n--) {
            if (n > log.size()) continue;

            // Check if the entry at index n has term == currentTerm
            // (1-based index n means 0-based index n-1)
            if (log.get(n - 1).term() != currentTerm.get()) {
                continue;
            }

            // Count how many peers have matchIndex >= n
            int replicationCount = 0;
            for (Map.Entry<String, Integer> entry : matchIndex.entrySet()) {
                if (entry.getValue() >= n) {
                    replicationCount++;
                }
            }

            if (replicationCount >= majority) {
                // Found a commit point
                commitIndex.set(n);
                kvStore.updateCommitIndex(n);
                kvStore.applyCommittedEntries();
                return;
            }
        }
    }

    // ==================== TIMERS ====================

    /**
     * Schedule a randomized election timer.
     */
    private void scheduleElectionTimer() {
        long timeoutMs = ELECTION_TIMEOUT_MIN_MS +
                (long) (Math.random() * (ELECTION_TIMEOUT_MAX_MS - ELECTION_TIMEOUT_MIN_MS));

        ThreadFactory electionFactory = Thread.ofVirtual().name("election-timer-" + nodeId).factory();
        ScheduledExecutorService newTimer = Executors.newSingleThreadScheduledExecutor(electionFactory);

        // Schedule outside synchronized block to avoid race conditions
        ScheduledFuture<?> future = null;
        try {
            future = newTimer.schedule(this::onElectionTimeout, timeoutMs, TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            newTimer.shutdownNow();
            return;
        }

        synchronized (timerLock) {
            cancelElectionTimerLocked();
            electionTimer = newTimer;
            electionTimeoutFuture = future;
        }
    }

    /**
     * Cancel the election timer.
     */
    private void cancelElectionTimer() {
        synchronized (timerLock) {
            cancelElectionTimerLocked();
        }
    }

    /**
     * Cancel the election timer (must be called while holding timerLock).
     */
    private void cancelElectionTimerLocked() {
        if (electionTimeoutFuture != null) {
            electionTimeoutFuture.cancel(false);
            electionTimeoutFuture = null;
        }
        if (electionTimer != null) {
            electionTimer.shutdownNow();
            electionTimer = null;
        }
    }

    /**
     * Called when the election timer fires.
     */
    private void onElectionTimeout() {
        NodeState state = currentState.get();
        if (state == NodeState.LEADER) {
            return;
        }

        if (state == NodeState.FOLLOWER) {
            // First timeout: transition to CANDIDATE
            setState(NodeState.CANDIDATE);
            currentTerm.incrementAndGet();
        } else {
            // Re-election (CANDIDATE timeout): increment term
            currentTerm.incrementAndGet();
        }

        votedFor.set(nodeId);
        electionInProgress = true;
        votesReceived = new ConcurrentHashMap<>();
        // Count our own vote
        votesReceived.put(nodeId, new AtomicInteger(1));

        // Send VOTE_REQUEST to all peers
        sendVoteRequestToAll();

        // Reschedule election timer (in case we don't win)
        scheduleElectionTimer();
    }

    /**
     * Start the heartbeat loop.
     */
    private void startHeartbeat() {
        cancelHeartbeat();

        ThreadFactory heartbeatFactory = r -> Thread.ofVirtual().name("heartbeat-" + nodeId).start(r);
        heartbeatExecutor = Executors.newSingleThreadScheduledExecutor(heartbeatFactory);

        // Use a Runnable wrapper that doesn't call reExecutePeriodic internally
        Runnable heartbeatTask = new Runnable() {
            private final Runnable delegate = () -> {
                if (currentState.get() == NodeState.LEADER) {
                    sendHeartbeatToAll();
                }
            };

            @Override
            public void run() {
                try {
                    delegate.run();
                } finally {
                    // Manually reschedule outside the worker thread context
                    ScheduledFuture<?> next = heartbeatExecutor.schedule(this, HEARTBEAT_INTERVAL_MS * 2, TimeUnit.MILLISECONDS);
                    synchronized (timerLock) {
                        heartbeatFuture = next;
                    }
                }
            }
        };

        heartbeatFuture = heartbeatExecutor.schedule(heartbeatTask, HEARTBEAT_INTERVAL_MS, TimeUnit.MILLISECONDS);
    }

    /**
     * Cancel the heartbeat loop.
     */
    private void cancelHeartbeat() {
        if (heartbeatFuture != null) {
            heartbeatFuture.cancel(false);
            heartbeatFuture = null;
        }
        if (heartbeatExecutor != null) {
            heartbeatExecutor.shutdownNow();
            heartbeatExecutor = null;
        }
    }

    // ==================== OUTGOING RPCs ====================

    /**
     * Send VOTE_REQUEST to all peers.
     */
    private void sendVoteRequestToAll() {
        int lastLogIndex = kvStore.getLog().size();
        int lastLogTerm = 0;
        if (!kvStore.getLog().isEmpty()) {
            LogEntry lastEntry = kvStore.getLog().get(lastLogIndex - 1);
            lastLogTerm = lastEntry.term();
        }

        Message voteRequest = Message.voteRequest(
                nodeId, currentTerm.get(), lastLogIndex, lastLogTerm
        );

        for (Map.Entry<String, String> entry : peers.entrySet()) {
            String peerId = entry.getKey();
            if (peerId.equals(nodeId)) continue;
            sendAsync(entry.getValue(), voteRequest);
        }
    }

    /**
     * Send HEARTBEAT to all peers (includes log replication).
     */
    private void sendHeartbeatToAll() {
        if (currentState.get() != NodeState.LEADER) {
            return;
        }

        // Replicate log entries to all peers (including empty heartbeats)
        for (Map.Entry<String, String> entry : peers.entrySet()) {
            String peerId = entry.getKey();
            if (peerId.equals(nodeId)) continue;

            Integer ni = nextIndex.get(peerId);
            if (ni == null || ni <= 0) continue;

            List<LogEntry> log = kvStore.getLog();
            int lastLogIndex = log.size();

            // Build entries to send (starting from nextIndex)
            List<LogEntry> entriesToSend = new ArrayList<>();
            int startIndex = Math.min(ni - 1, lastLogIndex);
            for (int i = startIndex; i < lastLogIndex; i++) {
                entriesToSend.add(log.get(i));
            }

            // Serialize entries
            String[] serializedEntries = new String[entriesToSend.size()];
            for (int i = 0; i < entriesToSend.size(); i++) {
                serializedEntries[i] = entriesToSend.get(i).serialize();
            }

            // Determine prevLogIndex and prevLogTerm
            int prevLogIndex = ni - 1;
            int prevLogTerm = 0;
            if (prevLogIndex > 0) {
                prevLogTerm = log.get(prevLogIndex - 1).term();
            }

            int leaderCommit = commitIndex.get();
            Message appendEntries = Message.appendEntries(
                    nodeId, currentTerm.get(), prevLogIndex, prevLogTerm,
                    serializedEntries, leaderCommit
            );

            String peerAddress = entry.getValue();
            sendReplicationMessage(peerId, peerAddress, appendEntries);
        }
    }

    /**
     * Send a message asynchronously to a peer address (host:port) and process the response.
     */
    private void sendAsync(String peerAddress, Message message) {
        executor.submit(() -> {
            try (Socket socket = new Socket()) {
                socket.setSoTimeout(PEER_CONNECT_TIMEOUT_MS);
                // Parse host:port
                String[] parts = peerAddress.split(":", 2);
                if (parts.length != 2) return;
                String host = parts[0];
                int port = Integer.parseInt(parts[1]);
                socket.connect(new InetSocketAddress(host, port), PEER_CONNECT_TIMEOUT_MS);
                ProtocolCodec.encode(message, socket.getOutputStream());
                // Read the response
                Message response = ProtocolCodec.decode(socket.getInputStream());
                // Process the response based on message type
                if (message.type() == MessageType.VOTE_REQUEST && response.type() == MessageType.VOTE_RESPONSE) {
                    handleVoteResponse(response);
                }
                // HEARTBEAT responses are fire-and-forget (no action needed)
            } catch (IOException e) {
                // Silently ignore failed RPCs
            }
        });
    }

    // ==================== STATE TRANSITIONS ====================

    /**
     * Set the node state (with timer management).
     */
    private void setState(NodeState newState) {
        NodeState oldState = currentState.getAndSet(newState);
        if (oldState != newState) {
            // If transitioning FROM LEADER, cancel heartbeat
            if (oldState == NodeState.LEADER) {
                cancelHeartbeat();
            }
            // If transitioning TO FOLLOWER from CANDIDATE, cancel election timer and reschedule
            if (oldState == NodeState.CANDIDATE && newState == NodeState.FOLLOWER) {
                cancelElectionTimer();
            }
            // If transitioning to CANDIDATE, cancel old election timer
            if (newState == NodeState.CANDIDATE) {
                cancelElectionTimer();
            }
        }
    }
}
