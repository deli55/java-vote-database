package com.raftkv.protocol;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;

/**
 * 4-byte big-endian length-prefixed text/string encoder and decoder.
 *
 * <p>The protocol format is:
 * <pre>
 * +--------+------------------+
 * | LENGTH | PAYLOAD (UTF-8)  |
 * | 4 bytes| Variable length  |
 * +--------+------------------+
 * </pre>
 */
public final class ProtocolCodec {

    private static final int HEAD_LENGTH = 4;
    private static final int MAX_MESSAGE_SIZE = 1024 * 1024; // 1 MB max message size

    private ProtocolCodec() {
        // Utility class - no instantiation
    }

    /**
     * Encode a message by writing its serialized form with a 4-byte length header.
     *
     * @param message the message to encode
     * @param output the output stream to write to
     * @throws IOException if an I/O error occurs
     */
    public static void encode(Message message, OutputStream output) throws IOException {
        String payload = message.serialize();
        byte[] bytes = payload.getBytes(java.nio.charset.StandardCharsets.UTF_8);

        if (bytes.length > MAX_MESSAGE_SIZE) {
            throw new IOException("Message too large: " + bytes.length + " bytes");
        }

        ByteBuffer buffer = ByteBuffer.allocate(HEAD_LENGTH + bytes.length);
        buffer.order(ByteOrder.BIG_ENDIAN);
        buffer.putInt(bytes.length);
        buffer.put(bytes);
        buffer.flip();

        output.write(buffer.array(), buffer.position(), buffer.remaining());
        output.flush();
    }

    /**
     * Encode a string by writing it with a 4-byte length header.
     *
     * @param payload the string to encode
     * @param output the output stream to write to
     * @throws IOException if an I/O error occurs
     */
    public static void encode(String payload, OutputStream output) throws IOException {
        byte[] bytes = payload.getBytes(java.nio.charset.StandardCharsets.UTF_8);

        if (bytes.length > MAX_MESSAGE_SIZE) {
            throw new IOException("Message too large: " + bytes.length + " bytes");
        }

        ByteBuffer buffer = ByteBuffer.allocate(HEAD_LENGTH + bytes.length);
        buffer.order(ByteOrder.BIG_ENDIAN);
        buffer.putInt(bytes.length);
        buffer.put(bytes);
        buffer.flip();

        output.write(buffer.array(), buffer.position(), buffer.remaining());
        output.flush();
    }

    /**
     * Decode a message by reading the length header and then the payload.
     *
     * @param input the input stream to read from
     * @return the decoded message
     * @throws IOException if an I/O error occurs or the message is malformed
     */
    public static Message decode(InputStream input) throws IOException {
        String payload = decodeString(input);
        return parseMessage(payload);
    }

    /**
     * Decode a string by reading the length header and then the payload.
     *
     * @param input the input stream to read from
     * @return the decoded string
     * @throws IOException if an I/O error occurs or the message is malformed
     */
    public static String decodeString(InputStream input) throws IOException {
        ByteBuffer lengthBuffer = ByteBuffer.allocate(HEAD_LENGTH);
        lengthBuffer.order(ByteOrder.BIG_ENDIAN);

        // Read the full 4-byte header
        int headerLength = readFully(input, lengthBuffer.array(), 0, HEAD_LENGTH);
        if (headerLength != HEAD_LENGTH) {
            throw new IOException("Incomplete header: only read " + headerLength + " of 4 bytes");
        }

        int payloadLength = lengthBuffer.getInt(0);

        if (payloadLength < 0 || payloadLength > MAX_MESSAGE_SIZE) {
            throw new IOException("Invalid message length: " + payloadLength);
        }

        if (payloadLength == 0) {
            return "";
        }

        byte[] payloadBytes = new byte[payloadLength];
        int totalRead = readFully(input, payloadBytes, 0, payloadLength);
        if (totalRead != payloadLength) {
            throw new IOException("Incomplete payload: only read " + totalRead + " of " + payloadLength + " bytes");
        }

        return new String(payloadBytes, java.nio.charset.StandardCharsets.UTF_8);
    }

    /**
     * Read the exact number of bytes from the input stream.
     *
     * @param input the input stream
     * @param buffer the buffer to read into
     * @param offset the offset in the buffer
     * @param length the number of bytes to read
     * @return the number of bytes read
     * @throws IOException if an I/O error occurs or the stream ends prematurely
     */
    private static int readFully(InputStream input, byte[] buffer, int offset, int length) throws IOException {
        int totalRead = 0;
        while (totalRead < length) {
            int bytesRead = input.read(buffer, offset + totalRead, length - totalRead);
            if (bytesRead < 0) {
                throw new IOException("Stream ended unexpectedly at byte " + (totalRead + offset));
            }
            totalRead += bytesRead;
        }
        return totalRead;
    }

    /**
     * Parse a serialized message string back into a Message object.
     *
     * @param serialized the serialized message string
     * @return the parsed Message
     */
    private static Message parseMessage(String serialized) {
        // Parse: MESSAGE{type=...,key=...,value=...,term=...,requestId=...,serverId=...,
        // leaderId=...,prevLogIndex=...,prevLogTerm=...,entries=[...],commitIndex=...,
        // voteGranted=...,status=...,errorMessage=...}

        if (serialized == null || !serialized.startsWith("MESSAGE{") || !serialized.endsWith("}")) {
            throw new IllegalArgumentException("Invalid message format: " + serialized);
        }

        // Extract the content between { and }
        String content = serialized.substring("MESSAGE{".length(), serialized.length() - 1);

        // Initialize all fields with default values
        MessageType type = null;
        CommandType commandType = null;
        String key = null;
        String value = null;
        int term = 0;
        String requestId = null;
        String serverId = null;
        String leaderId = null;
        int prevLogIndex = 0;
        int prevLogTerm = 0;
        String[] entries = null;
        int commitIndex = 0;
        boolean voteGranted = false;
        String status = null;
        String errorMessage = null;
        int matchIndex = 0;

        // Split by comma while respecting bracket depth (entries contain commas inside brackets)
        List<String> fields = splitFields(content);
        for (String fieldWithEqual : fields) {
            fieldWithEqual = fieldWithEqual.trim();
            if (fieldWithEqual.isEmpty()) continue;

            int eqIdx = fieldWithEqual.indexOf('=');
            if (eqIdx <= 0) continue; // Skip fields without proper key=value format

            String fieldName = fieldWithEqual.substring(0, eqIdx).trim();
            String fieldValue = fieldWithEqual.substring(eqIdx + 1).trim();

            if (fieldName.isEmpty() || fieldValue.isEmpty()) continue;

            switch (fieldName) {
                case "type" -> {
                    type = parseEnumValue(fieldValue, MessageType.class, MessageType::fromString);
                }
                case "commandType" -> {
                    commandType = parseEnumValue(fieldValue, CommandType.class, CommandType::fromString);
                }
                case "term" -> {
                    term = parseIntValue(fieldValue);
                }
                case "voteGranted" -> {
                    voteGranted = "true".equalsIgnoreCase(fieldValue);
                }
                case "prevLogIndex" -> {
                    prevLogIndex = parseIntValue(fieldValue);
                }
                case "prevLogTerm" -> {
                    prevLogTerm = parseIntValue(fieldValue);
                }
                case "commitIndex" -> {
                    commitIndex = parseIntValue(fieldValue);
                }
                case "key" -> {
                    key = parseStringValue(fieldValue);
                }
                case "value" -> {
                    value = parseStringValue(fieldValue);
                }
                case "requestId" -> {
                    requestId = parseStringValue(fieldValue);
                }
                case "serverId" -> {
                    serverId = parseStringValue(fieldValue);
                }
                case "leaderId" -> {
                    leaderId = parseStringValue(fieldValue);
                }
                case "status" -> {
                    status = parseStringValue(fieldValue);
                }
                case "errorMessage" -> {
                    errorMessage = parseStringValue(fieldValue);
                }
                case "entries" -> {
                    entries = parseArrayValue(fieldValue);
                }
                case "matchIndex" -> {
                    matchIndex = parseIntValue(fieldValue);
                }
                default -> {
                    // Skip unknown fields
                }
            }
        }

        return new Message(
                type, commandType, key, value, term, requestId,
                serverId, leaderId, prevLogIndex, prevLogTerm,
                entries, commitIndex, voteGranted, status, errorMessage, matchIndex
        );
    }

    @SuppressWarnings("unchecked")
    private static <E extends Enum<?>> E parseEnumValue(String value, Class<E> clazz, java.util.function.Function<String, E> parser) {
        if (value.startsWith("\"") && value.endsWith("\"") && value.length() >= 2) {
            value = value.substring(1, value.length() - 1);
        }
        return parser.apply(value);
    }

    private static int parseIntValue(String value) {
        if (value.startsWith("\"") && value.endsWith("\"") && value.length() >= 2) {
            value = value.substring(1, value.length() - 1);
        }
        try {
            return Integer.parseInt(value.trim());
        } catch (NumberFormatException e) {
            return 0;
        }
    }

    private static String parseStringValue(String value) {
        if (value.startsWith("\"") && value.endsWith("\"") && value.length() >= 2) {
            return value.substring(1, value.length() - 1);
        }
        return value.isEmpty() ? null : value;
    }

    private static String[] parseArrayValue(String value) {
        if (!value.startsWith("[") || !value.endsWith("]") || value.length() < 2) {
            return null;
        }
        String inner = value.substring(1, value.length() - 1).trim();
        if (inner.isEmpty()) {
            return null;
        }
        // Parse entries that may contain commas inside brackets (e.g., LOG{...})
        List<String> result = new ArrayList<>();
        int start = 0;
        while (start < inner.length()) {
            int depth = 0;
            int end = start;
            while (end < inner.length()) {
                char c = inner.charAt(end);
                if (c == '{' || c == '[') {
                    depth++;
                } else if (c == '}' || c == ']') {
                    depth--;
                } else if (c == ',' && depth == 0) {
                    String entry = inner.substring(start, end).trim();
                    if (!entry.isEmpty()) {
                        result.add(entry);
                    }
                    start = end + 1;
                    break;
                }
                end++;
            }
            if (end >= inner.length()) {
                String entry = inner.substring(start).trim();
                if (!entry.isEmpty()) {
                    result.add(entry);
                }
                break;
            }
        }
        return result.toArray(new String[0]);
    }

    private static int findFieldValueEnd(String content, int start) {
        if (start < content.length() && content.charAt(start) == '"') {
            int end = findStringEnd(content, start + 1);
            return end + 1;
        }
        // Non-quoted value
        int end = start;
        while (end < content.length() && content.charAt(end) != ',' && content.charAt(end) != '}') {
            end++;
        }
        return end;
    }

    private static int findStringEnd(String content, int start) {
        for (int i = start; i < content.length(); i++) {
            if (content.charAt(i) == '"') {
                return i;
            }
        }
        return content.length();
    }

    private static int findNumericEnd(String content, int start) {
        int end = start;
        while (end < content.length() && Character.isDigit(content.charAt(end))) {
            end++;
        }
        return end;
    }

    /**
     * Split the content by commas while respecting bracket depth.
     * This is needed because entries like LOG{...} contain commas inside brackets.
     */
    private static List<String> splitFields(String content) {
        List<String> result = new ArrayList<>();
        int start = 0;
        int depth = 0;
        for (int i = 0; i < content.length(); i++) {
            char c = content.charAt(i);
            if (c == '{' || c == '[') {
                depth++;
            } else if (c == '}' || c == ']') {
                depth--;
            } else if (c == ',' && depth == 0) {
                String field = content.substring(start, i).trim();
                if (!field.isEmpty()) {
                    result.add(field);
                }
                start = i + 1;
            }
        }
        // Don't forget the last field
        String lastField = content.substring(start).trim();
        if (!lastField.isEmpty()) {
            result.add(lastField);
        }
        return result;
    }
}
