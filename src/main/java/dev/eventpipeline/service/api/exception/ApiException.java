package dev.eventpipeline.service.api.exception;

import org.springframework.http.HttpStatus;

import java.net.URI;

/**
 * Exception that carries an explicit HTTP status and a stable RFC 7807 {@code type} URI, translated by
 * the global exception handler into a {@code ProblemDetail} response. Use the factory methods so each
 * status is paired with its matching {@link ProblemTypes} identifier.
 */
public class ApiException extends RuntimeException {

    private final HttpStatus status;
    private final URI type;

    private ApiException(HttpStatus status, URI type, String message) {
        super(message);
        this.status = status;
        this.type = type;
    }

    /** Event with the requested id does not exist. 404 with {@link ProblemTypes#EVENT_NOT_FOUND}. */
    public static ApiException notFound(String message) {
        return new ApiException(HttpStatus.NOT_FOUND, ProblemTypes.EVENT_NOT_FOUND, message);
    }

    /** eventId already exists with a different payload. 409 with {@link ProblemTypes#EVENT_PAYLOAD_CONFLICT}. */
    public static ApiException conflict(String message) {
        return new ApiException(HttpStatus.CONFLICT, ProblemTypes.EVENT_PAYLOAD_CONFLICT, message);
    }

    /** Invalid request input not covered by Bean Validation. 400 with {@link ProblemTypes#VALIDATION_FAILED}. */
    public static ApiException badRequest(String message) {
        return new ApiException(HttpStatus.BAD_REQUEST, ProblemTypes.VALIDATION_FAILED, message);
    }

    public HttpStatus getStatus() {
        return status;
    }

    public URI getType() {
        return type;
    }
}
