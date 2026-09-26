package com.raftkv.protocol;

/**
 * Represents the type of command to execute on the key-value store.
 */
public enum CommandType {
    PUT,
    GET,
    DELETE,
    NO_OP;

    /**
     * Parse a string to a CommandType.
     *
     * @param value the string representation
     * @return the corresponding CommandType
     * @throws IllegalArgumentException if the value is not recognized
     */
    public static CommandType fromString(String value) {
        try {
            return valueOf(value);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Unknown CommandType: " + value, e);
        }
    }
}
