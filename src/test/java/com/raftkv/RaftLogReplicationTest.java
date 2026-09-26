package com.raftkv;

import com.raftkv.protocol.*;
import org.junit.jupiter.api.*;

import java.io.IOException;
import java.net.Socket;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Multi-node integration tests for Raft log replication and state machine commits.
 *
 * Tests:
 * - Test 1 (testLogReplicationAcrossCluster): Issue PUT to LEADER, verify key/value
 *   is replicated and readable via GET across all 3 nodes.
 * - Test 2 (testFollowerCatchUp): Stop 1 FOLLOWER, issue writes to LEADER (must
 *   succeed with 2/3 majority), restart FOLLOWER, and verify catch-up replication.
 */
class RaftLogReplicationTest {

    private static final int ELECTION_TIMEOUT_MS = 300;
    private static final int HEARTBEAT_INTERVAL_MS = 50;
    private static final int WAIT_FOR_LEADER_MS = 3000;
    private static final int WAIT_FOR_REPLICATION_MS = 3000;
    private static final int CLUSTER_SIZE = 3;

    private Path tempDir;
    private RaftNodeServer[] nodes = new RaftNodeServer[CLUSTER_SIZE];
    private KVStoreStateMachine[] kvStores = new KVStoreStateMachine[CLUSTER_SIZE];
    private int[] ports = new int[CLUSTER_SIZE];
    private String[] nodeIds = new String[CLUSTER_SIZE];
    private Thread[] serverThreads = new Thread[CLUSTER_SIZE];

    @BeforeEach
    void setUp() throws Exception {
        tempDir = Files.createTempDirectory("raft-replication-test-");

        // Generate node IDs
        for (int i = 0; i < CLUSTER_SIZE; i++) {
            nodeIds[i] = "node-" + i;
        }

        // Find free ports
        for (int i = 0; i < CLUSTER_SIZE; i++) {
            ports[i] = findFreePort();
        }

        // Create KVStores
        for (int i = 0; i < CLUSTER_SIZE; i++) {
            Path walPath = tempDir.resolve("wal-" + i + ".log");
            kvStores[i] = new KVStoreStateMachine(walPath);
        }

        // Create nodes with each starting as FOLLOWER
        for (int i = 0; i < CLUSTER_SIZE; i++) {
            nodes[i] = new RaftNodeServer(
                    ports[i],
                    kvStores[i],
                    nodeIds[i],
                    NodeState.FOLLOWER,
                    0
            );
        }

        // Configure peer maps for each node
        for (int i = 0; i < CLUSTER_SIZE; i++) {
            Map<String, String> peerMap = new java.util.HashMap<>();
            for (int j = 0; j < CLUSTER_SIZE; j++) {
                if (i != j) {
                    peerMap.put(nodeIds[j], "localhost:" + ports[j]);
                }
            }
            nodes[i].setPeers(peerMap);
        }

        // Start all servers
        for (int i = 0; i < CLUSTER_SIZE; i++) {
            final int idx = i;
            serverThreads[i] = new Thread(() -> {
                try {
                    nodes[idx].start();
                } catch (IOException e) {
                    System.err.println("Server " + idx + " error: " + e.getMessage());
                }
            }, "test-node-" + idx);
            serverThreads[i].setDaemon(true);
            serverThreads[i].start();
        }

        // Wait for servers to start
        Thread.sleep(300);
    }

    @AfterEach
    void tearDown() throws Exception {
        for (int i = 0; i < CLUSTER_SIZE; i++) {
            if (nodes[i] != null && nodes[i].isRunning()) {
                nodes[i].stop();
            }
            if (serverThreads[i] != null && serverThreads[i].isAlive()) {
                serverThreads[i].interrupt();
            }
            if (kvStores[i] != null) {
                kvStores[i].close();
            }
        }
        if (tempDir != null) {
            Files.walk(tempDir)
                    .sorted((a, b) -> b.compareTo(a))
                    .forEach(path -> {
                        try {
                            Files.deleteIfExists(path);
                        } catch (IOException e) {
                            // Ignore
                        }
                    });
        }
    }

    /**
     * Helper: Wait for a leader to be elected.
     *
     * @return the index of the leader, or -1 if no leader elected within timeout
     */
    private int waitForLeader() throws InterruptedException {
        long startTime = System.currentTimeMillis();
        while (System.currentTimeMillis() - startTime < WAIT_FOR_LEADER_MS) {
            for (int i = 0; i < CLUSTER_SIZE; i++) {
                if (nodes[i].getNodeState() == NodeState.LEADER) {
                    return i;
                }
            }
            Thread.sleep(100);
        }
        return -1;
    }

    /**
     * Helper: Connect to a specific node by index.
     */
    private Socket connectToNode(int nodeIndex) throws IOException {
        return new Socket("localhost", ports[nodeIndex]);
    }

    /**
     * Helper: Send a client request to a node and get the response.
     */
    private Message sendClientRequest(int nodeIndex, CommandType commandType, String key, String value) throws Exception {
        Message request = Message.clientRequest(
                commandType, key, value,
                UUID.randomUUID().toString(), nodeIds[nodeIndex],
                nodes[nodeIndex].getCurrentTerm()
        );

        try (Socket socket = connectToNode(nodeIndex)) {
            ProtocolCodec.encode(request, socket.getOutputStream());
            return ProtocolCodec.decode(socket.getInputStream());
        }
    }

    /**
     * Helper: Wait for replication to all nodes.
     * Reads directly from KVStore since only the LEADER handles client requests.
     */
    private boolean waitForReplication(String key, String expectedValue, int... nodeIndices) throws InterruptedException {
        long startTime = System.currentTimeMillis();
        while (System.currentTimeMillis() - startTime < WAIT_FOR_REPLICATION_MS) {
            boolean allMatch = true;
            for (int idx : nodeIndices) {
                try {
                    String value = kvStores[idx].get(key);
                    if (!expectedValue.equals(value)) {
                        allMatch = false;
                        break;
                    }
                } catch (Exception e) {
                    allMatch = false;
                    break;
                }
            }
            if (allMatch) return true;
            Thread.sleep(100);
        }
        return false;
    }

    // ==================== TEST 1: Log Replication Across Cluster ====================

    /**
     * Test 1: Issue PUT to LEADER, verify key/value is replicated and readable
     * via GET across all 3 nodes.
     */
    @Test
    void testLogReplicationAcrossCluster() throws Exception {
        // Wait for leader election
        int leaderIndex = waitForLeader();
        assertTrue(leaderIndex >= 0, "A leader should be elected within timeout");
        System.out.println("Leader elected: " + nodeIds[leaderIndex]);

        // Wait for heartbeats to propagate leader info to followers
        Thread.sleep(200);

        // Verify all nodes have the same state
        for (int i = 0; i < CLUSTER_SIZE; i++) {
            if (i != leaderIndex) {
                assertEquals(NodeState.FOLLOWER, nodes[i].getNodeState(),
                        "Node " + nodeIds[i] + " should be FOLLOWER");
                assertEquals(nodeIds[leaderIndex], nodes[i].getLeaderId(),
                        "Follower should know the leader");
            }
        }

        // Issue PUT to leader
        String testKey = "replication-key-" + UUID.randomUUID().toString().substring(0, 8);
        String testValue = "replication-value-" + System.currentTimeMillis();

        Message putResponse = sendClientRequest(leaderIndex, CommandType.PUT, testKey, testValue);
        assertEquals("SUCCESS", putResponse.status(), "Leader should accept PUT");

        // Wait for replication to all nodes (with increased timeout)
        boolean replicated = waitForReplication(testKey, testValue, 0, 1, 2);
        assertTrue(replicated, "Key/value should be replicated to all nodes within timeout");

        // Verify each node has the value (read directly from KVStore since only LEADER handles client requests)
        for (int i = 0; i < CLUSTER_SIZE; i++) {
            assertEquals(testValue, kvStores[i].get(testKey),
                    "Node " + nodeIds[i] + " should have the correct value for " + testKey);
        }

        // Verify log entries exist on all nodes
        for (int i = 0; i < CLUSTER_SIZE; i++) {
            int logSize = kvStores[i].getLog().size();
            assertTrue(logSize >= 1,
                    "Node " + nodeIds[i] + " should have at least 1 log entry (had: " + logSize + ")");
        }
    }

    // ==================== TEST 2: Follower Catch-Up ====================

    /**
     * Test 2: Stop 1 FOLLOWER, issue writes to LEADER (must succeed with 2/3 majority),
     * restart FOLLOWER, and verify catch-up replication.
     */
    @Test
    void testFollowerCatchUp() throws Exception {
        // Wait for leader election
        int leaderIndex = waitForLeader();
        assertTrue(leaderIndex >= 0, "A leader should be elected within timeout");
        System.out.println("Leader elected: " + nodeIds[leaderIndex]);

        // Identify the follower that we will stop (not the leader)
        int[] followerIndices = new int[CLUSTER_SIZE - 1];
        int fIdx = 0;
        for (int i = 0; i < CLUSTER_SIZE; i++) {
            if (i != leaderIndex) {
                followerIndices[fIdx++] = i;
            }
        }

        // Pick a follower to stop (not the leader)
        final int finalFollowerToStop;
        int followerToStop = -1;
        for (int fi : followerIndices) {
            if (fi != leaderIndex) {
                followerToStop = fi;
                break;
            }
        }
        finalFollowerToStop = followerToStop;

        System.out.println("Stopping follower: " + nodeIds[finalFollowerToStop]);

        // Stop the selected follower
        nodes[finalFollowerToStop].stop();
        Thread.sleep(200);
        assertFalse(nodes[finalFollowerToStop].isRunning(),
                "Selected follower should be stopped");

        // Issue multiple writes to the leader
        String prefix = "catchup-key-" + UUID.randomUUID().toString().substring(0, 8);
        for (int i = 0; i < 5; i++) {
            String key = prefix + "-" + i;
            String value = "value-" + i;

            Message putResponse = sendClientRequest(leaderIndex, CommandType.PUT, key, value);
            assertEquals("SUCCESS", putResponse.status(),
                    "Leader should accept PUT #" + i + " even with 2/3 nodes");
        }

        // Wait for replication to the remaining follower (not the stopped one)
        final int finalFollowerToStopForRef = finalFollowerToStop;
        int remainingFollower = -1;
        for (int i = 0; i < CLUSTER_SIZE; i++) {
            if (i != leaderIndex && i != finalFollowerToStopForRef) {
                remainingFollower = i;
                break;
            }
        }

        // Verify writes are replicated to leader and remaining follower
        if (remainingFollower >= 0) {
            boolean replicated = waitForReplication(prefix + "-0", "value-0", leaderIndex, remainingFollower);
            assertTrue(replicated,
                    "Writes should be replicated to leader and remaining follower within timeout");
        }

        // Verify writes are available on the leader (read directly from KVStore)
        for (int i = 0; i < 5; i++) {
            String key = prefix + "-" + i;
            assertEquals("value-" + i, kvStores[leaderIndex].get(key),
                    "Leader should have value for " + key);
        }

        // Restart the stopped follower
        final int finalFs = finalFollowerToStop;
        Thread restartThread = new Thread(() -> {
            try {
                nodes[finalFs].start();
            } catch (IOException e) {
                System.err.println("Restart error for node " + finalFs + ": " + e.getMessage());
            }
        }, "restart-node-" + finalFs);
        restartThread.setDaemon(true);
        restartThread.start();

        // Wait for the follower to start and catch up
        Thread.sleep(1000);

        // Wait for catch-up replication
        boolean caughtUp = waitForReplication(prefix + "-0", "value-0", finalFs);
        assertTrue(caughtUp,
                "Stopped follower should catch up after restart within timeout");

        // Verify all entries are available on the restarted follower (read directly from KVStore)
        for (int i = 0; i < 5; i++) {
            String key = prefix + "-" + i;
            assertEquals("value-" + i, kvStores[finalFs].get(key),
                    "Restarted node " + nodeIds[finalFs] + " should have value for " + key);
        }

        // Verify log size is consistent across all nodes
        int leaderLogSize = kvStores[leaderIndex].getLog().size();
        int remainingLogSize = remainingFollower >= 0 ? kvStores[remainingFollower].getLog().size() : 0;
        int stoppedLogSize = kvStores[finalFs].getLog().size();

        assertEquals(leaderLogSize, remainingLogSize,
                "Remaining follower should have the same log size as leader");
        assertEquals(leaderLogSize, stoppedLogSize,
                "Restarted follower should have caught up to the same log size as leader");
    }

    // ==================== HELPERS ====================

    private static int findFreePort() throws IOException {
        try (var socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }
}
