package dev.eventpipeline.service.api.exception;

import org.springframework.http.HttpStatus;

/**
 * Thrown when a requested event does not exist. Mapped to 404 Not Found with type
 * {@link ProblemTypes#EVENT_NOT_FOUND}.
 */
public class EventNotFoundException extends ApiException {

    public EventNotFoundException(String message) {
        super(HttpStatus.NOT_FOUND, ProblemTypes.EVENT_NOT_FOUND, message);
    }
}
