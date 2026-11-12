package dev.eventpipeline.service.api.dto;

import dev.eventpipeline.service.domain.Event;
import dev.eventpipeline.service.domain.EventStatus;
import dev.eventpipeline.service.domain.EventType;

import java.math.BigDecimal;
import java.time.Instant;

/**
 * Response body for a single event, used by both the create (after lookup is not needed) and get endpoints.
 */
public record EventResponse(
        String eventId,
        String customerId,
        EventType type,
        BigDecimal amount,
        String currency,
        EventStatus status,
        Instant createdAt,
        Instant processedAt
) {

    public static EventResponse from(Event event) {
        return new EventResponse(
                event.getEventId(),
                event.getCustomerId(),
                event.getType(),
                event.getAmount(),
                event.getCurrency(),
                event.getStatus(),
                event.getCreatedAt(),
                event.getProcessedAt());
    }
}
