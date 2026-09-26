package com.raftkv;

import com.raftkv.protocol.*;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.net.Socket;
import java.net.ServerSocket;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests for the RaftNodeServer.
 *
 * Tests cover:
 * <ul>
 *   <li>PUT/GET operations</li>
 *   <li>DELETE operations</li>
 *   <li>WAL recovery replay</li>
 *   <li>Concurrent Virtual Thread client connections</li>
 * </ul>
 */
class RaftNodeServerTest {

    @TempDir
    Path tempDir;

    private RaftNodeServer server;
    private KVStoreStateMachine kvStore;
    private int port;

    @BeforeEach
    void setUp() throws Exception {
        // Find a free port
        port = findFreePort();
        Path walPath = tempDir.resolve("test-wal.log");

        // Create a fresh KVStore
        kvStore = new KVStoreStateMachine(walPath);

        // Create and start the server (LEADER for single-node testing)
        server = new RaftNodeServer(
                port,
                kvStore,
                "test-server-" + UUID.randomUUID().toString().substring(0, 8),
                NodeState.LEADER,
                0
        );

        // Start server in a background thread
        Thread serverThread = new Thread(() -> {
            try {
                server.start();
            } catch (IOException e) {
                // Ignore during test
            }
        }, "test-server");
        serverThread.setDaemon(true);
        serverThread.start();

        // Wait for server to start
        Thread.sleep(100);
    }

    @AfterEach
    void tearDown() throws Exception {
        if (server != null && server.isRunning()) {
            server.stop();
        }
        if (kvStore != null) {
            kvStore.close();
        }
    }

    @Test
    void testPutAndGet() throws Exception {
        String key = "test-key";
        String value = "test-value";

        // Send PUT request
        Message putRequest = Message.clientRequest(
                CommandType.PUT, key, value,
                UUID.randomUUID().toString(), "test-server", 0
        );

        try (var socket = connectToServer()) {
            ProtocolCodec.encode(putRequest, socket.getOutputStream());
            Message putResponse = ProtocolCodec.decode(socket.getInputStream());

            assertEquals(MessageType.CLIENT_RESPONSE, putResponse.type());
            assertEquals("SUCCESS", putResponse.status());
            assertEquals(value, putResponse.value());
        }

        // Send GET request
        Message getRequest = Message.clientRequest(
                CommandType.GET, key, null,
                UUID.randomUUID().toString(), "test-server", 0
        );

        try (var socket = connectToServer()) {
            ProtocolCodec.encode(getRequest, socket.getOutputStream());
            Message getResponse = ProtocolCodec.decode(socket.getInputStream());

            assertEquals(MessageType.CLIENT_RESPONSE, getResponse.type());
            assertEquals("SUCCESS", getResponse.status());
            assertEquals(value, getResponse.value());
        }
    }

    @Test
    void testPutAndGetNonExistentKey() throws Exception {
        String key = "non-existent-key";

        Message getRequest = Message.clientRequest(
                CommandType.GET, key, null,
                UUID.randomUUID().toString(), "test-server", 0
        );

        try (var socket = connectToServer()) {
            ProtocolCodec.encode(getRequest, socket.getOutputStream());
            Message getResponse = ProtocolCodec.decode(socket.getInputStream());

            assertEquals(MessageType.CLIENT_RESPONSE, getResponse.type());
            assertEquals("SUCCESS", getResponse.status());
            assertEquals("", getResponse.value()); // Empty for non-existent keys
        }
    }

    @Test
    void testDelete() throws Exception {
        String key = "key-to-delete";
        String value = "temporary-value";

        // First PUT the value
        Message putRequest = Message.clientRequest(
                CommandType.PUT, key, value,
                UUID.randomUUID().toString(), "test-server", 0
        );

        try (var socket = connectToServer()) {
            ProtocolCodec.encode(putRequest, socket.getOutputStream());
            Message putResponse = ProtocolCodec.decode(socket.getInputStream());
            assertEquals("SUCCESS", putResponse.status());
        }

        // Verify it exists
        Message getRequest = Message.clientRequest(
                CommandType.GET, key, null,
                UUID.randomUUID().toString(), "test-server", 0
        );

        try (var socket = connectToServer()) {
            ProtocolCodec.encode(getRequest, socket.getOutputStream());
            Message getResponse = ProtocolCodec.decode(socket.getInputStream());
            assertEquals(value, getResponse.value());
        }

        // DELETE the key
        Message deleteRequest = Message.clientRequest(
                CommandType.DELETE, key, null,
                UUID.randomUUID().toString(), "test-server", 0
        );

        try (var socket = connectToServer()) {
            ProtocolCodec.encode(deleteRequest, socket.getOutputStream());
            Message deleteResponse = ProtocolCodec.decode(socket.getInputStream());
            assertEquals("SUCCESS", deleteResponse.status());
        }

        // Verify it's deleted
        Message getAfterDelete = Message.clientRequest(
                CommandType.GET, key, null,
                UUID.randomUUID().toString(), "test-server", 0
        );

        try (var socket = connectToServer()) {
            ProtocolCodec.encode(getAfterDelete, socket.getOutputStream());
            Message response = ProtocolCodec.decode(socket.getInputStream());
            assertEquals("", response.value()); // Empty after deletion
        }
    }

    @Test
    void testWalRecoveryReplay() throws Exception {
        Path walPath = tempDir.resolve("recovery-wal.log");

        // Create first server and add some data
        KVStoreStateMachine store1 = new KVStoreStateMachine(walPath);
        RaftNodeServer server1 = new RaftNodeServer(
                findFreePort(),
                store1,
                "server-1",
                NodeState.LEADER,
                0
        );

        Thread server1Thread = new Thread(() -> {
            try {
                server1.start();
            } catch (IOException e) {
                // Ignore
            }
        }, "server-1");
        server1Thread.setDaemon(true);
        server1Thread.start();

        // Wait for server 1 to start
        Thread.sleep(100);

        // Add some data
        String[] keys = {"key1", "key2", "key3"};
        String[] values = {"value1", "value2", "value3"};

        for (int i = 0; i < keys.length; i++) {
            Message putRequest = Message.clientRequest(
                    CommandType.PUT, keys[i], values[i],
                    UUID.randomUUID().toString(), "server-1", 0
            );

            try (var socket = connectToServer(server1.getPort())) {
                ProtocolCodec.encode(putRequest, socket.getOutputStream());
                Message response = ProtocolCodec.decode(socket.getInputStream());
                assertEquals("SUCCESS", response.status());
            }
        }

        // Stop server 1
        server1.stop();
        store1.close();

        // Verify WAL file exists
        assertTrue(Files.exists(walPath), "WAL file should exist");
        assertTrue(Files.size(walPath) > 0, "WAL file should not be empty");

        // Create new server with same WAL
        KVStoreStateMachine store2 = new KVStoreStateMachine(walPath);
        RaftNodeServer server2 = new RaftNodeServer(
                findFreePort(),
                store2,
                "server-2",
                NodeState.LEADER,
                0
        );

        Thread server2Thread = new Thread(() -> {
            try {
                server2.start();
            } catch (IOException e) {
                // Ignore
            }
        }, "server-2");
        server2Thread.setDaemon(true);
        server2Thread.start();

        // Wait for server 2 to start
        Thread.sleep(100);

        // Verify all keys were recovered
        for (int i = 0; i < keys.length; i++) {
            Message getRequest = Message.clientRequest(
                    CommandType.GET, keys[i], null,
                    UUID.randomUUID().toString(), "server-2", 0
            );

            try (var socket = connectToServer(server2.getPort())) {
                ProtocolCodec.encode(getRequest, socket.getOutputStream());
                Message response = ProtocolCodec.decode(socket.getInputStream());
                assertEquals(values[i], response.value(), "Key " + keys[i] + " should be recovered");
            }
        }

        // Cleanup
        server2.stop();
        store2.close();
    }

    @Test
    void testConcurrentConnections() throws Exception {
        int numClients = 10;
        int operationsPerClient = 5;
        ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();
        CountDownLatch allDone = new CountDownLatch(numClients);
        AtomicInteger successCount = new AtomicInteger(0);
        AtomicInteger errorCount = new AtomicInteger(0);

        // Each client performs multiple operations
        for (int client = 0; client < numClients; client++) {
            final int clientId = client;
            executor.submit(() -> {
                try {
                    for (int op = 0; op < operationsPerClient; op++) {
                        String key = "key-client" + clientId + "-op" + op;
                        String value = "value-client" + clientId + "-op" + op;

                        // PUT
                        Message putRequest = Message.clientRequest(
                                CommandType.PUT, key, value,
                                UUID.randomUUID().toString(), "test-server", 0
                        );

                        try (var socket = connectToServer()) {
                            ProtocolCodec.encode(putRequest, socket.getOutputStream());
                            Message response = ProtocolCodec.decode(socket.getInputStream());
                            if ("SUCCESS".equals(response.status())) {
                                successCount.incrementAndGet();
                            } else {
                                errorCount.incrementAndGet();
                            }
                        }

                        // GET
                        Message getRequest = Message.clientRequest(
                                CommandType.GET, key, null,
                                UUID.randomUUID().toString(), "test-server", 0
                        );

                        try (var socket = connectToServer()) {
                            ProtocolCodec.encode(getRequest, socket.getOutputStream());
                            Message response = ProtocolCodec.decode(socket.getInputStream());
                            if ("SUCCESS".equals(response.status()) && value.equals(response.value())) {
                                successCount.incrementAndGet();
                            } else {
                                errorCount.incrementAndGet();
                            }
                        }
                    }
                } catch (Exception e) {
                    errorCount.incrementAndGet();
                } finally {
                    allDone.countDown();
                }
            });
        }

        // Wait for all clients to complete (with timeout)
        assertTrue(allDone.await(30, TimeUnit.SECONDS), "All clients should complete within timeout");

        // Verify all operations succeeded
        assertEquals(0, errorCount.get(), "There should be no errors");
        assertEquals(numClients * operationsPerClient * 2, successCount.get(), "All operations should succeed");

        // Verify store size
        assertEquals(numClients * operationsPerClient, kvStore.size());

        executor.shutdown();
        assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
    }

    @Test
    void testNoOpCommand() throws Exception {
        Message noOpRequest = Message.clientRequest(
                CommandType.NO_OP, null, null,
                UUID.randomUUID().toString(), "test-server", 0
        );

        try (var socket = connectToServer()) {
            ProtocolCodec.encode(noOpRequest, socket.getOutputStream());
            Message response = ProtocolCodec.decode(socket.getInputStream());

            assertEquals(MessageType.CLIENT_RESPONSE, response.type());
            assertEquals("SUCCESS", response.status());
        }
    }

    @Test
    void testMessageSerialization() throws Exception {
        Message original = Message.clientRequest(
                CommandType.PUT, "test-key", "test-value",
                "request-123", "server-1", 1
        );

        // Test encoding and decoding
        java.io.ByteArrayOutputStream baos = new java.io.ByteArrayOutputStream();
        ProtocolCodec.encode(original, baos);

        Message decoded = ProtocolCodec.decode(new java.io.ByteArrayInputStream(baos.toByteArray()));

        assertEquals(original.type(), decoded.type());
        assertEquals(original.commandType(), decoded.commandType());
        assertEquals(original.key(), decoded.key());
        assertEquals(original.value(), decoded.value());
        assertEquals(original.requestId(), decoded.requestId());
    }

    @Test
    void testProtocolCodec() throws Exception {
        // Test encoding and decoding a string
        String testString = "Hello, World!";
        java.io.ByteArrayOutputStream baos = new java.io.ByteArrayOutputStream();
        ProtocolCodec.encode(testString, baos);

        String decoded = ProtocolCodec.decodeString(
                new java.io.ByteArrayInputStream(baos.toByteArray())
        );

        assertEquals(testString, decoded);
    }

    // Helper method to connect to the server
    private Socket connectToServer() throws IOException {
        return connectToServer(port);
    }

    private Socket connectToServer(int serverPort) throws IOException {
        return new Socket("localhost", serverPort);
    }

    /**
     * Find a free port on the system.
     *
     * @return a free port number
     */
    private static int findFreePort() throws IOException {
        try (var socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }
}
