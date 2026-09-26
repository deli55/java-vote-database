package com.raftkv;

import java.io.*;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

import com.raftkv.protocol.CommandType;

/**
 * Thread-safe in-memory key-value store with Write-Ahead Logging (WAL) and recovery replay logic.
 *
 * <p>This component implements a state machine that applies log entries to a ConcurrentHashMap
 * and supports recovery by replaying the WAL on startup.
 */
public class KVStoreStateMachine {

    private final ConcurrentHashMap<String, String> store = new ConcurrentHashMap<>();
    private final List<LogEntry> log = new ArrayList<>();
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock(true); // fair lock
    private final Path walPath;
    private final FileWriter walWriter;
    private volatile int lastApplied = 0;
    private volatile int commitIndex = 0;
    // Next log index for atomic index assignment under write lock
    private int nextIndex = 1;

    /**
     * Create a KVStoreStateMachine with WAL enabled.
     *
     * @param walPath the path to the WAL file
     * @throws IOException if an I/O error occurs
     */
    public KVStoreStateMachine(Path walPath) throws IOException {
        this.walPath = walPath.toAbsolutePath().normalize();
        Files.createDirectories(walPath.getParent());

        // Recover from WAL if file exists (MUST happen before opening FileWriter)
        boolean fileExists = Files.exists(this.walPath);
        if (fileExists) {
            recoverFromWAL();
        }
        
        // Now open the FileWriter for future writes
        this.walWriter = new FileWriter(this.walPath.toFile(), fileExists);
    }

    /**
     * Create a KVStoreStateMachine with WAL disabled (in-memory only).
     */
    public KVStoreStateMachine() {
        this.walPath = null;
        this.walWriter = null;
    }

    /**
     * Apply a log entry to the state machine. The index is assigned atomically
     * from an internal counter under the write lock to prevent race conditions.
     *
     * @param entry the log entry to apply
     * @return the response value (for GET operations) or null
     * @throws Exception if applying the entry fails
     */
    public String applyLogEntry(LogEntry entry) throws Exception {
        lock.writeLock().lock();
        try {
            // Assign index atomically from internal counter
            int assignedIndex = nextIndex++;
            LogEntry indexedEntry = new LogEntry(assignedIndex, entry.term(), entry.commandType(), entry.key(), entry.value());

            // Write to WAL first (Write-Ahead Log)
            writeToWAL(indexedEntry);

            // Apply the entry to the state machine
            String result = switch (indexedEntry.commandType()) {
                case PUT -> {
                    store.put(indexedEntry.key(), indexedEntry.value());
                    yield null;
                }
                case GET -> {
                    yield store.getOrDefault(indexedEntry.key(), null);
                }
                case DELETE -> {
                    store.remove(indexedEntry.key());
                    yield null;
                }
                case NO_OP -> null;
            };

            // Update log
            log.add(indexedEntry);
            lastApplied = assignedIndex;

            return result;
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Get and increment the next log index atomically. Used when an external
     * caller needs to know the index in advance (e.g., for building messages).
     *
     * @return the next log index
     */
    public int getNextIndex() {
        lock.writeLock().lock();
        try {
            return nextIndex++;
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Append a log entry with a specific index (used when receiving entries from leader).
     * This method does NOT increment nextIndex - it uses the provided index.
     *
     * @param entry the log entry with a pre-assigned index
     * @return the response value (for GET operations) or null
     * @throws Exception if applying the entry fails
     */
    public String appendEntryWithIndex(LogEntry entry) throws Exception {
        lock.writeLock().lock();
        try {
            LogEntry indexedEntry = entry; // Use the entry with its pre-assigned index

            // Write to WAL first (Write-Ahead Log)
            writeToWAL(indexedEntry);

            // Apply the entry to the state machine
            String result = switch (indexedEntry.commandType()) {
                case PUT -> {
                    store.put(indexedEntry.key(), indexedEntry.value());
                    yield null;
                }
                case GET -> {
                    yield store.getOrDefault(indexedEntry.key(), null);
                }
                case DELETE -> {
                    store.remove(indexedEntry.key());
                    yield null;
                }
                case NO_OP -> null;
            };

            // Update log
            log.add(indexedEntry);
            lastApplied = indexedEntry.index();

            // Update nextIndex if needed
            if (indexedEntry.index() >= nextIndex) {
                nextIndex = indexedEntry.index() + 1;
            }

            return result;
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Reset the next log index after recovery.
     *
     * @param index the new next index
     */
    public void resetNextIndex(int index) {
        lock.writeLock().lock();
        try {
            nextIndex = index;
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Get the value associated with the given key.
     *
     * @param key the key to look up
     * @return the value, or null if not found
     */
    public String get(String key) {
        lock.readLock().lock();
        try {
            return store.getOrDefault(key, null);
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Check if the store contains the given key.
     *
     * @param key the key to check
     * @return true if the key exists, false otherwise
     */
    public boolean containsKey(String key) {
        lock.readLock().lock();
        try {
            return store.containsKey(key);
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Delete a key from the store.
     *
     * @param key the key to delete
     * @return the previous value, or null if not present
     */
    public String delete(String key) {
        lock.writeLock().lock();
        try {
            return store.remove(key);
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Put a key-value pair into the store.
     *
     * @param key   the key
     * @param value the value
     */
    public void put(String key, String value) {
        lock.writeLock().lock();
        try {
            store.put(key, value);
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Get the current commit index.
     *
     * @return the commit index
     */
    public int getCommitIndex() {
        lock.readLock().lock();
        try {
            return commitIndex;
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Update the commit index.
     *
     * @param index the new commit index
     */
    public void updateCommitIndex(int index) {
        lock.writeLock().lock();
        try {
            commitIndex = index;
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Apply all committed entries from the log to the state machine.
     * This is called by the Raft node when the commit index advances.
     * Only applies entries that have not been applied yet (index > lastApplied).
     *
     * @return the new lastApplied index
     */
    public int applyCommittedEntries() {
        lock.writeLock().lock();
        try {
            int currentLastApplied = lastApplied;
            int newLastApplied = currentLastApplied;

            for (int i = currentLastApplied; i < log.size(); i++) {
                LogEntry entry = log.get(i);
                // Only apply entries up to the commit index
                if (i + 1 > commitIndex) {
                    break;
                }
                // Apply the entry to the state machine
                switch (entry.commandType()) {
                    case PUT -> store.put(entry.key(), entry.value());
                    case GET -> {
                        // GET is a no-op for state machine
                    }
                    case DELETE -> store.remove(entry.key());
                    case NO_OP -> {
                        // NO_OP is a no-op
                    }
                }
                newLastApplied = i + 1;
            }

            lastApplied = newLastApplied;
            return newLastApplied;
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Get the last applied index.
     *
     * @return the last applied index
     */
    public int getLastApplied() {
        lock.readLock().lock();
        try {
            return lastApplied;
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Get the current log entries.
     *
     * @return an unmodifiable list of log entries
     */
    public List<LogEntry> getLog() {
        lock.readLock().lock();
        try {
            return Collections.unmodifiableList(new ArrayList<>(log));
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Get the size of the in-memory store.
     *
     * @return the number of entries in the store
     */
    public int size() {
        lock.readLock().lock();
        try {
            return store.size();
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Get all keys in the store.
     *
     * @return an unmodifiable set of keys
     */
    public Set<String> keys() {
        lock.readLock().lock();
        try {
            return Collections.unmodifiableSet(new HashSet<>(store.keySet()));
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Clear the store and log. Used for testing.
     */
    public void clear() {
        lock.writeLock().lock();
        try {
            store.clear();
            log.clear();
            lastApplied = 0;
            commitIndex = 0;
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Write an entry to the Write-Ahead Log.
     *
     * @param entry the log entry to write
     */
    private void writeToWAL(LogEntry entry) {
        if (walWriter == null) {
            return;
        }
        try {
            String line = entry.serialize();
            walWriter.write(line + System.lineSeparator());
            walWriter.flush();
        } catch (IOException e) {
            throw new RuntimeException("Failed to write to WAL", e);
        }
    }

    /**
     * Recover from the Write-Ahead Log by replaying all entries.
     */
    private void recoverFromWAL() {
        lock.writeLock().lock();
        try {
            List<String> lines = Files.readAllLines(walPath);
            int index = 1;

            for (String line : lines) {
                if (line.isBlank() || !line.startsWith("LOG{")) {
                    continue;
                }

                LogEntry entry = parseLogEntry(line, index);
                if (entry != null) {
                    // Apply the entry
                    switch (entry.commandType()) {
                        case PUT -> store.put(entry.key(), entry.value());
                        case GET -> {
                            // GET in WAL is a no-op for recovery
                        }
                        case DELETE -> store.remove(entry.key());
                        case NO_OP -> {
                            // NO_OP is a no-op
                        }
                    }
                    log.add(entry);
                    lastApplied = index;
                    index++;
                }
            }
            // Set nextIndex to the next value after recovery
            nextIndex = index;
        } catch (IOException e) {
            throw new RuntimeException("Failed to recover from WAL", e);
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Parse a log entry from a serialized string.
     *
     * @param line the serialized log entry string
     * @param index the log index to assign
     * @return the parsed LogEntry, or null if parsing fails
     */
    private LogEntry parseLogEntry(String line, int index) {
        try {
            // Parse: LOG{index=...,term=...,commandType=...,key=...,value=...}
            String content = line.substring(4, line.length() - 1);

            String commandType = null;
            String key = null;
            String value = null;
            int term = 0;

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
            // Skip malformed entries during recovery
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
     * Close the state machine and flush the WAL.
     */
    public void close() {
        if (walWriter != null) {
            try {
                walWriter.flush();
                walWriter.close();
            } catch (IOException e) {
                throw new RuntimeException("Failed to close WAL", e);
            }
        }
    }
}
