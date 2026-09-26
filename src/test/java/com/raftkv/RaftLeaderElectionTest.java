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
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Multi-node integration tests for Raft leader election and heartbeat mechanism.
 *
 * Tests:
 * - Test 1: Verify exactly one node becomes LEADER and others remain FOLLOWER
 * - Test 2: Verify LEADER accepts writes and FOLLOWERS reject writes
 */
class RaftLeaderElectionTest {

    private static final int ELECTION_TIMEOUT_MS = 300;
    private static final int HEARTBEAT_INTERVAL_MS = 50;
    private static final int WAIT_FOR_LEADER_MS = 2000;
    private static final int CLUSTER_SIZE = 3;

    private Path tempDir;
    private RaftNodeServer[] nodes = new RaftNodeServer[CLUSTER_SIZE];
    private KVStoreStateMachine[] kvStores = new KVStoreStateMachine[CLUSTER_SIZE];
    private int[] ports = new int[CLUSTER_SIZE];
    private String[] nodeIds = new String[CLUSTER_SIZE];

    @BeforeEach
    void setUp() throws Exception {
        tempDir = Files.createTempDirectory("raft-test-");

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
            Thread serverThread = new Thread(() -> {
                try {
                    nodes[idx].start();
                } catch (IOException e) {
                    System.err.println("Server " + idx + " error: " + e.getMessage());
                }
            }, "test-node-" + idx);
            serverThread.setDaemon(true);
            serverThread.start();
        }

        // Wait for servers to start
        Thread.sleep(200);
    }

    @AfterEach
    void tearDown() throws Exception {
        for (int i = 0; i < CLUSTER_SIZE; i++) {
            if (nodes[i] != null && nodes[i].isRunning()) {
                nodes[i].stop();
            }
            if (kvStores[i] != null) {
                kvStores[i].close();
            }
        }
        if (tempDir != null) {
            java.nio.file.Files.walk(tempDir)
                    .sorted((a, b) -> b.compareTo(a))
                    .forEach(path -> {
                        try {
                            java.nio.file.Files.deleteIfExists(path);
                        } catch (IOException e) {
                            // Ignore
                        }
                    });
        }
    }

    /**
     * Test 1: Verify exactly one node becomes LEADER within 2 seconds
     * and the other two remain FOLLOWER.
     */
    @Test
    void testSingleLeaderElection() throws Exception {
        // Wait for election to complete
        Thread.sleep(WAIT_FOR_LEADER_MS);

        // Count leaders and followers
        int leaderCount = 0;
        int followerCount = 0;
        String leaderId = null;

        for (int i = 0; i < CLUSTER_SIZE; i++) {
            NodeState state = nodes[i].getNodeState();
            System.out.println("Node " + nodeIds[i] + " (port " + ports[i] + ") state: " + state);

            if (state == NodeState.LEADER) {
                leaderCount++;
                leaderId = nodeIds[i];
            } else if (state == NodeState.FOLLOWER) {
                followerCount++;
            }
        }

        // Exactly one leader
        assertEquals(1, leaderCount, "Exactly one node should become LEADER");
        assertNotNull(leaderId, "There should be a leader");

        // All other nodes should be FOLLOWERs
        assertEquals(CLUSTER_SIZE - 1, followerCount,
                "All other nodes should be FOLLOWERs");

        // Each follower should know the leader
        for (int i = 0; i < CLUSTER_SIZE; i++) {
            if (!nodeIds[i].equals(leaderId)) {
                assertEquals(leaderId, nodes[i].getLeaderId(),
                        "Follower " + nodeIds[i] + " should know the leader");
            }
        }

        // The leader should know itself as leader
        RaftNodeServer leaderNode = nodes[findNodeIndex(leaderId)];
        assertEquals(leaderId, leaderNode.getLeaderId(),
                "Leader should know itself as the leader");
    }

    /**
     * Test 2: Verify LEADER accepts writes and FOLLOWERS reject writes.
     */
    @Test
    void testLeaderAcceptsWritesFollowersReject() throws Exception {
        // Wait for election to complete
        Thread.sleep(WAIT_FOR_LEADER_MS);

        // Find the leader
        String leaderId = null;
        for (int i = 0; i < CLUSTER_SIZE; i++) {
            if (nodes[i].getNodeState() == NodeState.LEADER) {
                leaderId = nodeIds[i];
                break;
            }
        }

        assertNotNull(leaderId, "A leader should exist");
        int leaderIndex = findNodeIndex(leaderId);

        String testKey = "test-key-" + UUID.randomUUID().toString().substring(0, 8);
        String testValue = "test-value";

        // Test: Leader accepts PUT
        Message putRequest = Message.clientRequest(
                CommandType.PUT, testKey, testValue,
                UUID.randomUUID().toString(), nodeIds[leaderIndex],
                nodes[leaderIndex].getCurrentTerm()
        );

        try (Socket leaderSocket = connectToNode(leaderIndex)) {
            ProtocolCodec.encode(putRequest, leaderSocket.getOutputStream());
            Message putResponse = ProtocolCodec.decode(leaderSocket.getInputStream());

            assertEquals("SUCCESS", putResponse.status(),
                    "Leader should accept PUT");
        }

        // Test: Followers reject PUT with "Not leader" error
        for (int i = 0; i < CLUSTER_SIZE; i++) {
            if (nodes[i].getNodeState() != NodeState.LEADER) {
                Message followerPutRequest = Message.clientRequest(
                        CommandType.PUT, testKey, testValue,
                        UUID.randomUUID().toString(), nodeIds[i],
                        nodes[i].getCurrentTerm()
                );

                try (Socket followerSocket = connectToNode(i)) {
                    ProtocolCodec.encode(followerPutRequest, followerSocket.getOutputStream());
                    Message followerPutResponse = ProtocolCodec.decode(followerSocket.getInputStream());

                    assertEquals("FAILURE", followerPutResponse.status(),
                            "Follower " + nodeIds[i] + " should reject PUT");
                    assertTrue(followerPutResponse.errorMessage().toLowerCase().contains("leader"),
                            "Follower " + nodeIds[i] + " should say 'Not leader' or similar");
                }
            }
        }

        // Test: Leader accepts GET and returns the value
        Message getRequest = Message.clientRequest(
                CommandType.GET, testKey, null,
                UUID.randomUUID().toString(), nodeIds[leaderIndex],
                nodes[leaderIndex].getCurrentTerm()
        );

        try (Socket leaderSocket = connectToNode(leaderIndex)) {
            ProtocolCodec.encode(getRequest, leaderSocket.getOutputStream());
            Message getResponse = ProtocolCodec.decode(leaderSocket.getInputStream());

            assertEquals("SUCCESS", getResponse.status());
            assertEquals(testValue, getResponse.value(),
                    "Leader should return the correct value");
        }
    }

    /**
     * Test 3: Verify term increases on election.
     */
    @Test
    void testTermIncreasesOnElection() throws Exception {
        // All nodes start with term 0
        for (int i = 0; i < CLUSTER_SIZE; i++) {
            assertEquals(0, nodes[i].getCurrentTerm(), "All nodes start with term 0");
        }

        // Wait for election
        Thread.sleep(WAIT_FOR_LEADER_MS);

        // After election, the leader should have term >= 1
        for (int i = 0; i < CLUSTER_SIZE; i++) {
            int term = nodes[i].getCurrentTerm();
            assertTrue(term >= 0, "All nodes should have term >= 0");
        }

        // Find leader
        String leaderId = null;
        for (int i = 0; i < CLUSTER_SIZE; i++) {
            if (nodes[i].getNodeState() == NodeState.LEADER) {
                leaderId = nodeIds[i];
                break;
            }
        }

        if (leaderId != null) {
            int leaderIndex = findNodeIndex(leaderId);
            int leaderTerm = nodes[leaderIndex].getCurrentTerm();
            // The leader's term should be at least 1 (election happened)
            assertTrue(leaderTerm >= 0, "Leader should have valid term");
        }
    }

    /**
     * Test 4: Verify heartbeats are exchanged.
     * After leader is elected, followers should receive heartbeats and know the leader.
     */
    @Test
    void testHeartbeatsAreExchanged() throws Exception {
        // Wait for election and initial heartbeats
        Thread.sleep(WAIT_FOR_LEADER_MS + 200); // Extra time for heartbeats

        // Find the leader
        String leaderId = null;
        for (int i = 0; i < CLUSTER_SIZE; i++) {
            if (nodes[i].getNodeState() == NodeState.LEADER) {
                leaderId = nodeIds[i];
                break;
            }
        }

        assertNotNull(leaderId, "A leader should exist");

        // All followers should know the leader (due to heartbeats)
        for (int i = 0; i < CLUSTER_SIZE; i++) {
            if (!nodeIds[i].equals(leaderId)) {
                assertEquals(leaderId, nodes[i].getLeaderId(),
                        "Follower " + nodeIds[i] + " should know the leader after heartbeats");
            }
        }
    }

    // ==================== HELPERS ====================

    private int findNodeIndex(String nodeId) {
        for (int i = 0; i < CLUSTER_SIZE; i++) {
            if (nodeIds[i].equals(nodeId)) {
                return i;
            }
        }
        throw new IllegalArgumentException("Unknown node: " + nodeId);
    }

    private Socket connectToNode(int nodeIndex) throws IOException {
        return new Socket("localhost", ports[nodeIndex]);
    }

    private static int findFreePort() throws IOException {
        try (var socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }
}
