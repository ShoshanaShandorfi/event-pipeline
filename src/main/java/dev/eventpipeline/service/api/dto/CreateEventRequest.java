package dev.eventpipeline.service.api.dto;

import dev.eventpipeline.service.api.validation.ValidCurrency;
import dev.eventpipeline.service.api.validation.ValidMetadata;
import dev.eventpipeline.service.domain.EventType;
import jakarta.validation.constraints.Digits;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import jakarta.validation.constraints.Size;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Map;

/**
 * Inbound payload for POST /api/events. Kept separate from the {@link dev.eventpipeline.service.domain.Event}
 * document so the wire format can evolve independently of persistence.
 */
public record CreateEventRequest(

        @NotBlank(message = "eventId must not be blank")
        @Size(max = 128, message = "eventId must be at most 128 characters")
        String eventId,

        @NotBlank(message = "customerId must not be blank")
        @Size(max = 128, message = "customerId must be at most 128 characters")
        String customerId,

        @NotNull(message = "type is required")
        EventType type,

        @NotNull(message = "amount is required")
        @Positive(message = "amount must be positive")
        @Digits(integer = 16, fraction = 6, message = "amount must have at most 16 integer digits and 6 decimal places")
        BigDecimal amount,

        @NotBlank(message = "currency is required")
        @ValidCurrency
        String currency,

        @NotNull(message = "timestamp is required")
        Instant timestamp,

        @ValidMetadata
        Map<String, String> metadata
) {
}
