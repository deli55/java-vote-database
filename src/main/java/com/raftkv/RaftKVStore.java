package com.raftkv;

import com.raftkv.protocol.CommandType;
import com.raftkv.protocol.Message;
import com.raftkv.protocol.ProtocolCodec;

import java.io.IOException;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.UUID;
import java.util.Scanner;

/**
 * CLI entry point / main launcher for the distributed key-value store.
 *
 * <p>This class provides:
 * <ul>
 *   <li>A CLI interface for interactive use</li>
 *   <li>A programmatic API for embedded use</li>
 * </ul>
 */
public class RaftKVStore {

    private static final String DEFAULT_PORT = "9000";
    private static final String DEFAULT_DATA_DIR = "./data";

    /**
     * Main entry point for the application.
     *
     * @param args command line arguments: [port] [data-dir]
     *             - port: the port to listen on (default: 9000)
     *             - data-dir: the directory for WAL files (default: ./data)
     * @throws IOException if an I/O error occurs
     * @throws InterruptedException if the program is interrupted
     */
    public static void main(String[] args) throws IOException, InterruptedException {
        int port;
        String dataDir;

        if (args.length == 0 || args.length == 1) {
            port = Integer.parseInt(args.length == 0 ? DEFAULT_PORT : args[0]);
            dataDir = DEFAULT_DATA_DIR;
        } else if (args.length == 2) {
            port = Integer.parseInt(args[0]);
            dataDir = args[1];
        } else {
            System.out.println("Usage: RaftKVStore [port] [data-dir]");
            System.out.println("  port:    The port to listen on (default: " + DEFAULT_PORT + ")");
            System.out.println("  data-dir: The directory for WAL files (default: " + DEFAULT_DATA_DIR + ")");
            return;
        }

        Path walPath = Path.of(dataDir, "wal.log").toAbsolutePath().normalize();
        Files.createDirectories(walPath.getParent());

        System.out.println("Starting RaftKVStore on port " + port + "...");
        System.out.println("WAL path: " + walPath);

        // Create the state machine with WAL
        KVStoreStateMachine kvStore = new KVStoreStateMachine(walPath);

        // Create and start the server
        RaftNodeServer server = new RaftNodeServer(
                port,
                kvStore,
                UUID.randomUUID().toString(),
                NodeState.FOLLOWER,
                0
        );

        // Start the server in a separate thread
        Thread serverThread = new Thread(() -> {
            try {
                server.start();
            } catch (IOException e) {
                System.err.println("Server error: " + e.getMessage());
            }
        }, "raft-server");
        serverThread.setDaemon(true);
        serverThread.start();

        System.out.println("Server started. Use 'connect [host] [port]' to connect to the server.");
        System.out.println("Or use commands directly: help");
        System.out.println("Type 'quit' to exit.");

        // Interactive CLI
        try (Scanner scanner = new Scanner(System.in)) {
            while (scanner.hasNextLine()) {
                String line = scanner.nextLine().trim();
                if (line.isBlank()) continue;

                if ("quit".equalsIgnoreCase(line) || "exit".equalsIgnoreCase(line)) {
                    break;
                }

                if ("help".equalsIgnoreCase(line)) {
                    printHelp();
                    continue;
                }

                if ("connect".equalsIgnoreCase(line)) {
                    System.out.println("Connected to local server on port " + port);
                    continue;
                }

                // Parse the command: PUT key value, GET key, DELETE key
                String[] parts = line.split("\\s+", 3);
                CommandType commandType = CommandType.fromString(parts[0].toUpperCase());

                try {
                    // Connect to local server and send request
                    try (Socket socket = new Socket("localhost", port)) {
                        String requestId = UUID.randomUUID().toString();
                        Message request = Message.clientRequest(
                                commandType,
                                parts.length > 1 ? parts[1] : null,
                                parts.length > 2 ? parts[2] : null,
                                requestId,
                                server.getNodeId(),
                                0
                        );

                        // Send request
                        ProtocolCodec.encode(request, socket.getOutputStream());

                        // Read response
                        Message response = ProtocolCodec.decode(socket.getInputStream());

                        if ("SUCCESS".equals(response.status())) {
                            if (commandType == CommandType.GET) {
                                System.out.println(response.value() != null ? "GET " + response.key() + " = " + response.value() : "GET " + response.key() + " = (null)");
                            } else {
                                System.out.println(commandType + " " + parts[1] + " = SUCCESS");
                            }
                        } else {
                            System.out.println("ERROR: " + response.errorMessage());
                        }
                    }
                } catch (IOException e) {
                    System.out.println("CONNECTION ERROR: " + e.getMessage());
                }
            }
        } finally {
            // Shutdown
            server.stop();
            kvStore.close();
            System.out.println("Server stopped.");
        }
    }

    /**
     * Print the help message.
     */
    private static void printHelp() {
        System.out.println("Available commands:");
        System.out.println("  PUT key value    - Store a value");
        System.out.println("  GET key          - Retrieve a value");
        System.out.println("  DELETE key       - Delete a key");
        System.out.println("  help             - Show this help message");
        System.out.println("  quit             - Exit the application");
    }
}
