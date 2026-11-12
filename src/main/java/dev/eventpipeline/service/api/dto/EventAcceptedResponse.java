package dev.eventpipeline.service.api.dto;

import dev.eventpipeline.service.domain.EventStatus;

/**
 * Response body for POST /api/events (202 Accepted). {@code duplicate} is {@code true} only when the
 * request replayed an already-persisted event with an identical payload.
 */
public record EventAcceptedResponse(String eventId, EventStatus status, boolean duplicate) {
}
