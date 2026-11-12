package dev.eventpipeline.service.api.exception;

import org.springframework.http.HttpStatus;

/**
 * Thrown for invalid request input that isn't covered by Bean Validation on the request body, such as
 * malformed or out-of-range query parameters. Mapped to 400 Bad Request with type
 * {@link ProblemTypes#VALIDATION_FAILED}.
 */
public class InvalidRequestException extends ApiException {

    public InvalidRequestException(String message) {
        super(HttpStatus.BAD_REQUEST, ProblemTypes.VALIDATION_FAILED, message);
    }
}
