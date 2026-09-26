package com.raftkv.protocol;

/**
 * Network message envelope for the Raft protocol communication.
 *
 * @param type        the type of message (e.g., CLIENT_REQUEST, VOTE_REQUEST)
 * @param commandType the type of command (e.g., PUT, GET) when applicable
 * @param key         the key for the operation
 * @param value       the value for the operation (null when not applicable)
 * @param term        the current Raft term
 * @param requestId   a unique request identifier for client requests
 * @param serverId    the ID of the originating server
 * @param leaderId    the ID of the current leader (empty for non-leader messages)
 * @param prevLogIndex the previous log index (for append entries)
 * @param prevLogTerm the previous log term (for append entries)
 * @param entries     log entries to append (for append entries)
 * @param commitIndex the leader's commit index
 * @param voteGranted whether the vote was granted (for vote responses)
 * @param status      the response status (for client responses)
 * @param errorMessage the error message (for failed responses)
 * @param matchIndex  the responder's matchIndex (for APPEND_ENTRIES_RESPONSE)
 */
public record Message(
        MessageType type,
        CommandType commandType,
        String key,
        String value,
        int term,
        String requestId,
        String serverId,
        String leaderId,
        int prevLogIndex,
        int prevLogTerm,
        String[] entries,
        int commitIndex,
        boolean voteGranted,
        String status,
        String errorMessage,
        int matchIndex
) {
    public Message {
        if (type == null) {
            throw new IllegalArgumentException("MessageType cannot be null");
        }
    }

    /**
     * Serialize this message to a JSON-like string representation.
     *
     * @return the serialized string
     */
    public String serialize() {
        StringBuilder sb = new StringBuilder();
        sb.append("MESSAGE{type=").append(type);
        if (commandType != null) sb.append(",commandType=").append(commandType);
        if (key != null) sb.append(",key=\"").append(escapeString(key)).append("\"");
        if (value != null) sb.append(",value=\"").append(escapeString(value)).append("\"");
        sb.append(",term=").append(term);
        if (requestId != null) sb.append(",requestId=\"").append(escapeString(requestId)).append("\"");
        if (serverId != null) sb.append(",serverId=\"").append(escapeString(serverId)).append("\"");
        if (leaderId != null) sb.append(",leaderId=\"").append(escapeString(leaderId)).append("\"");
        sb.append(",prevLogIndex=").append(prevLogIndex);
        sb.append(",prevLogTerm=").append(prevLogTerm);
        if (entries != null) {
            sb.append(",entries=[");
            for (int i = 0; i < entries.length; i++) {
                if (i > 0) sb.append(",");
                sb.append(entries[i]);
            }
            sb.append("]");
        }
        sb.append(",commitIndex=").append(commitIndex);
        sb.append(",voteGranted=").append(voteGranted);
        sb.append(",matchIndex=").append(matchIndex);
        if (status != null) sb.append(",status=\"").append(escapeString(status)).append("\"");
        if (errorMessage != null) sb.append(",errorMessage=\"").append(escapeString(errorMessage)).append("\"");
        sb.append("}");
        return sb.toString();
    }

    private static String escapeString(String s) {
        if (s == null) return "";
        return s.replace("\\", "\\\\").replace("\"", "\\\"");
    }

    /**
     * Create a client request message.
     *
     * @param commandType the command type
     * @param key         the key
     * @param value       the value
     * @param requestId   the request ID
     * @param serverId    the server ID
     * @param term        the current term
     * @return a new Message instance
     */
    public static Message clientRequest(CommandType commandType, String key, String value, String requestId, String serverId, int term) {
        return new Message(
                MessageType.CLIENT_REQUEST,
                commandType,
                key,
                value,
                term,
                requestId,
                serverId,
                null,
                0,
                0,
                null,
                0,
                false,
                null,
                null,
                0
        );
    }

    /**
     * Create a client response message.
     *
     * @param key          the key (for GET operations)
     * @param value        the value (for GET operations)
     * @param requestId    the request ID this response corresponds to
     * @param status       the status (SUCCESS or FAILURE)
     * @param errorMessage the error message (if any)
     * @param serverId     the server ID
     * @param term         the current term
     * @return a new Message instance
     */
    public static Message clientResponse(String key, String value, String requestId, String status, String errorMessage, String serverId, int term) {
        return new Message(
                MessageType.CLIENT_RESPONSE,
                null,
                key,
                value,
                term,
                requestId,
                serverId,
                null,
                0,
                0,
                null,
                0,
                false,
                status,
                errorMessage,
                0
        );
    }

    /**
     * Create a vote request message.
     *
     * @param candidateId the candidate's ID
     * @param term        the current term
     * @param lastLogIndex the candidate's last log index
     * @param lastLogTerm the candidate's last log term
     * @return a new Message instance
     */
    public static Message voteRequest(String candidateId, int term, int lastLogIndex, int lastLogTerm) {
        return new Message(
                MessageType.VOTE_REQUEST,
                null,
                null,
                null,
                term,
                null,
                candidateId,
                null,
                lastLogIndex,
                lastLogTerm,
                null,
                0,
                false,
                null,
                null,
                0
        );
    }

    /**
     * Create a vote response message.
     *
     * @param voterId     the voter's ID
     * @param term        the current term
     * @param voteGranted whether the vote was granted
     * @return a new Message instance
     */
    public static Message voteResponse(String voterId, int term, boolean voteGranted) {
        return new Message(
                MessageType.VOTE_RESPONSE,
                null,
                null,
                null,
                term,
                null,
                voterId,
                null,
                0,
                0,
                null,
                0,
                voteGranted,
                null,
                null,
                0
        );
    }

    /**
     * Create an append entries message.
     *
     * @param leaderId    the leader's ID
     * @param term        the current term
     * @param prevLogIndex the previous log index
     * @param prevLogTerm the previous log term
     * @param entries     the log entries to append
     * @param commitIndex the leader's commit index
     * @return a new Message instance
     */
    public static Message appendEntries(String leaderId, int term, int prevLogIndex, int prevLogTerm, String[] entries, int commitIndex) {
        return new Message(
                MessageType.APPEND_ENTRIES,
                null,
                null,
                null,
                term,
                null,
                leaderId,
                null,
                prevLogIndex,
                prevLogTerm,
                entries,
                commitIndex,
                false,
                null,
                null,
                0
        );
    }

    /**
     * Create an append entries response message.
     *
     * @param leaderId    the leader's ID
     * @param term        the current term
     * @param success     whether the append was successful
     * @param matchIndex  the responder's matchIndex (log length)
     * @return a new Message instance
     */
    public static Message appendEntriesResponse(String leaderId, int term, boolean success, int matchIndex) {
        return new Message(
                MessageType.APPEND_ENTRIES_RESPONSE,
                null,
                null,
                null,
                term,
                null,
                leaderId,
                null,
                0,
                0,
                null,
                0,
                false,
                success ? "SUCCESS" : "FAILURE",
                null,
                matchIndex
        );
    }

    /**
     * Create a heartbeat message (append entries with no entries).
     *
     * @param leaderId    the leader's ID
     * @param term        the current term
     * @param commitIndex the leader's commit index
     * @return a new Message instance
     */
    public static Message heartbeat(String leaderId, int term, int commitIndex) {
        return new Message(
                MessageType.HEARTBEAT,
                null,
                null,
                null,
                term,
                null,
                leaderId,
                null,
                0,
                0,
                null,
                commitIndex,
                false,
                null,
                null,
                0
        );
    }
}
