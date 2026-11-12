package dev.eventpipeline.service.api.exception;

import java.net.URI;

/**
 * Stable {@code urn:problem:*} identifiers used as the RFC 7807 {@code type} field for every error
 * response, replacing the default {@code about:blank}. Centralized here so the URIs are defined once
 * and referenced by constant rather than scattered as string literals across exception classes and the
 * global exception handler.
 */
public final class ProblemTypes {

    public static final URI EVENT_PAYLOAD_CONFLICT = URI.create("urn:problem:event-payload-conflict");
    public static final URI EVENT_NOT_FOUND = URI.create("urn:problem:event-not-found");
    public static final URI VALIDATION_FAILED = URI.create("urn:problem:validation-failed");
    public static final URI INTERNAL_ERROR = URI.create("urn:problem:internal-error");
    public static final URI SERVICE_UNAVAILABLE = URI.create("urn:problem:service-unavailable");

    private ProblemTypes() {
    }
}
