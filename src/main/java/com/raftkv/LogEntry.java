package com.raftkv;

import com.raftkv.protocol.CommandType;

/**
 * Represents a log entry in the Raft log.
 *
 * @param index     the index of this entry in the log
 * @param term      the term during which this entry was created
 * @param commandType the type of command (PUT, GET, DELETE, NO_OP)
 * @param key       the key for the operation
 * @param value     the value for the operation (null when not applicable)
 */
public record LogEntry(
        int index,
        int term,
        CommandType commandType,
        String key,
        String value
) {
    /**
     * Serialize this log entry to a string representation.
     *
     * @return the serialized string
     */
    public String serialize() {
        return "LOG{index=" + index + ",term=" + term + ",commandType=" + commandType +
                ",key=\"" + escapeString(key) + "\",value=\"" + escapeString(value) + "\"}";
    }

    private static String escapeString(String s) {
        if (s == null) return "";
        return s.replace("\\", "\\\\").replace("\"", "\\\"");
    }

    /**
     * Create a log entry from a command (index assigned later by the state machine).
     *
     * @param term        the current term
     * @param commandType the command type
     * @param key         the key
     * @param value       the value
     * @return a new LogEntry with index=0 (placeholder)
     */
    public static LogEntry fromCommand(int term, CommandType commandType, String key, String value) {
        return new LogEntry(0, term, commandType, key, value);
    }

    /**
     * Create a log entry from a command with explicit index.
     *
     * @param index       the log index
     * @param term        the current term
     * @param commandType the command type
     * @param key         the key
     * @param value       the value
     * @return a new LogEntry
     */
    public static LogEntry fromCommand(int index, int term, CommandType commandType, String key, String value) {
        return new LogEntry(index, term, commandType, key, value);
    }
}
