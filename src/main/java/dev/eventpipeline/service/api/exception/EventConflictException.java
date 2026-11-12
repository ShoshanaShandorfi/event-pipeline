package dev.eventpipeline.service.api.exception;

import org.springframework.http.HttpStatus;

/**
 * Thrown when an eventId already exists with a different payload than the one submitted. Mapped to
 * 409 Conflict with type {@link ProblemTypes#EVENT_PAYLOAD_CONFLICT}.
 */
public class EventConflictException extends ApiException {

    public EventConflictException(String message) {
        super(HttpStatus.CONFLICT, ProblemTypes.EVENT_PAYLOAD_CONFLICT, message);
    }
}
