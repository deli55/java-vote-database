package com.raftkv.protocol;

/**
 * Represents the type of message in the Raft protocol communication.
 */
public enum MessageType {
    CLIENT_REQUEST,
    CLIENT_RESPONSE,
    VOTE_REQUEST,
    VOTE_RESPONSE,
    APPEND_ENTRIES,
    APPEND_ENTRIES_RESPONSE,
    HEARTBEAT;

    /**
     * Parse a string to a MessageType.
     *
     * @param value the string representation
     * @return the corresponding MessageType
     * @throws IllegalArgumentException if the value is not recognized
     */
    public static MessageType fromString(String value) {
        try {
            return valueOf(value);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Unknown MessageType: " + value, e);
        }
    }
}
