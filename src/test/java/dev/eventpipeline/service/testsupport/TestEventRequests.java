package dev.eventpipeline.service.testsupport;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.Map;

/**
 * Builds minimal valid {@code POST /api/events} request bodies for tests, independent of the exact
 * {@code CreateEventRequest} field order/shape.
 */
public final class TestEventRequests {

    private TestEventRequests() {
    }

    public static String json(ObjectMapper objectMapper, String eventId, String customerId, String amount,
                               String timestamp, Map<String, String> metadata) {
        try {
            var node = objectMapper.createObjectNode();
            node.put("eventId", eventId);
            node.put("customerId", customerId);
            node.put("type", "PAYMENT");
            node.put("amount", amount);
            node.put("currency", "USD");
            node.put("timestamp", timestamp);
            if (metadata != null) {
                var metadataNode = objectMapper.createObjectNode();
                metadata.forEach(metadataNode::put);
                node.set("metadata", metadataNode);
            }
            return objectMapper.writeValueAsString(node);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
