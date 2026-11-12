package dev.eventpipeline.service.api.exception;

import org.springframework.http.HttpStatus;

import java.net.URI;

/**
 * Base type for exceptions that carry an explicit HTTP status and a stable RFC 7807 {@code type} URI,
 * translated by the global exception handler into a {@code ProblemDetail} response.
 */
public abstract class ApiException extends RuntimeException {

    private final HttpStatus status;
    private final URI type;

    protected ApiException(HttpStatus status, URI type, String message) {
        super(message);
        this.status = status;
        this.type = type;
    }

    public HttpStatus getStatus() {
        return status;
    }

    public URI getType() {
        return type;
    }
}
