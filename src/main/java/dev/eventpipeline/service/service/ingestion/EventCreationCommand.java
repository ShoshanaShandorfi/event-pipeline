package dev.eventpipeline.service.service.ingestion;

import dev.eventpipeline.service.domain.Event;
import dev.eventpipeline.service.domain.EventType;
import lombok.Builder;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Map;
import java.util.Objects;

/**
 * Service-layer command for creating an event, decoupled from the web {@code CreateEventRequest} DTO.
 */
@Builder
public record EventCreationCommand(
        String eventId,
        String customerId,
        EventType type,
        BigDecimal amount,
        String currency,
        Instant timestamp,
        Map<String, String> metadata
) {

    public EventCreationCommand {
        metadata = metadata == null ? Map.of() : metadata;
    }

    /**
     * Whether {@code existing} was created from an identical payload (used to tell an idempotent replay
     * from a conflicting reuse of the same eventId).
     */
    public boolean matches(Event existing) {
        Map<String, String> existingMetadata = existing.getMetadata() == null ? Map.of() : existing.getMetadata();

        return Objects.equals(existing.getCustomerId(), customerId)
                && Objects.equals(existing.getType(), type)
                && existing.getAmount() != null
                && amount != null
                && existing.getAmount().compareTo(amount) == 0
                && Objects.equals(existing.getCurrency(), currency)
                // Mongo stores Instant as a Date with millisecond precision, so truncate before comparing
                // to avoid treating a sub-millisecond difference as a conflicting payload.
                && existing.getTimestamp().truncatedTo(ChronoUnit.MILLIS)
                        .equals(timestamp.truncatedTo(ChronoUnit.MILLIS))
                && Objects.equals(existingMetadata, metadata);
    }
}
