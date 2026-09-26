package com.raftkv;

/**
 * Represents the state of a node in the Raft consensus algorithm.
 */
public enum NodeState {
    FOLLOWER,
    CANDIDATE,
    LEADER;

    /**
     * Parse a string to a NodeState.
     *
     * @param value the string representation
     * @return the corresponding NodeState
     * @throws IllegalArgumentException if the value is not recognized
     */
    public static NodeState fromString(String value) {
        try {
            return valueOf(value);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Unknown NodeState: " + value, e);
        }
    }
}
