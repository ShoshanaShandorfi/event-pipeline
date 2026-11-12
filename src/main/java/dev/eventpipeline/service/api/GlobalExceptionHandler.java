package dev.eventpipeline.service.api;

import dev.eventpipeline.service.api.exception.ApiException;
import dev.eventpipeline.service.api.exception.ProblemTypes;
import com.mongodb.MongoException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;
import org.springframework.http.HttpHeaders;
import org.springframework.http.ResponseEntity;
import org.springframework.dao.DataAccessException;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.web.HttpRequestMethodNotSupportedException;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.method.annotation.MethodArgumentTypeMismatchException;

import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Centralized exception translation producing RFC 7807 {@code ProblemDetail} responses. Internal
 * exception details (stack traces, driver-specific messages) are never included in the response body;
 * they are only written to the server logs. Every response carries a stable {@code urn:problem:*} type
 * (see {@link ProblemTypes}) instead of the RFC 7807 default {@code about:blank}, so clients can branch
 * on a stable identifier rather than parsing the human-readable {@code detail} message.
 */
@RestControllerAdvice
@Slf4j
public class GlobalExceptionHandler {

    @ExceptionHandler(ApiException.class)
    public ProblemDetail handleApiException(ApiException ex) {
        log.warn("Request rejected with {}: {}", ex.getStatus(), ex.getMessage());
        ProblemDetail problem = ProblemDetail.forStatusAndDetail(ex.getStatus(), ex.getMessage());
        problem.setType(ex.getType());
        return problem;
    }

    @ExceptionHandler(MethodArgumentNotValidException.class)
    public ProblemDetail handleValidationException(MethodArgumentNotValidException ex) {
        List<Map<String, String>> fieldErrors = ex.getBindingResult().getFieldErrors().stream()
                .map(fe -> Map.of(
                        "field", fe.getField(),
                        "message", Objects.requireNonNullElse(fe.getDefaultMessage(), "invalid value")))
                .toList();

        ProblemDetail problem = ProblemDetail.forStatusAndDetail(HttpStatus.BAD_REQUEST, "Request validation failed");
        problem.setType(ProblemTypes.VALIDATION_FAILED);
        problem.setProperty("errors", fieldErrors);
        log.warn("Validation failed for request: {}", fieldErrors);
        return problem;
    }

    @ExceptionHandler(HttpMessageNotReadableException.class)
    public ProblemDetail handleMalformedJson(HttpMessageNotReadableException ex) {
        // Parser diagnostics may contain rejected payload/metadata values; keep them out of logs.
        log.warn("Malformed request body");
        ProblemDetail problem = ProblemDetail.forStatusAndDetail(HttpStatus.BAD_REQUEST, "Malformed JSON request body");
        problem.setType(ProblemTypes.VALIDATION_FAILED);
        return problem;
    }

    @ExceptionHandler(MethodArgumentTypeMismatchException.class)
    public ProblemDetail handleTypeMismatch(MethodArgumentTypeMismatchException ex) {
        String detail = "Parameter '" + ex.getName() + "' has an invalid value";
        log.warn(detail);
        ProblemDetail problem = ProblemDetail.forStatusAndDetail(HttpStatus.BAD_REQUEST, detail);
        problem.setType(ProblemTypes.VALIDATION_FAILED);
        return problem;
    }

    @ExceptionHandler(HttpRequestMethodNotSupportedException.class)
    public ProblemDetail handleMethodNotSupported(HttpRequestMethodNotSupportedException ex) {
        return ProblemDetail.forStatusAndDetail(HttpStatus.METHOD_NOT_ALLOWED, "HTTP method not supported for this endpoint");
    }

    @ExceptionHandler(Exception.class)
    public ProblemDetail handleUnexpected(Exception ex) {
        log.error("Unhandled exception while processing request errorType={}", ex.getClass().getSimpleName());
        ProblemDetail problem = ProblemDetail.forStatusAndDetail(HttpStatus.INTERNAL_SERVER_ERROR, "An unexpected error occurred");
        problem.setType(ProblemTypes.INTERNAL_ERROR);
        return problem;
    }

    @ExceptionHandler({DataAccessException.class, MongoException.class})
    public ResponseEntity<ProblemDetail> handleDataAccess(RuntimeException ex) {
        // B: never publish after a failed insert. A client can retry the same id safely; an ambiguous
        // write may already have committed, in which case the relay recovers it. No driver message,
        // connection string, credentials or stack trace may cross the HTTP/log boundary here.
        log.warn("MongoDB operation unavailable; returning 503 ({})", ex.getClass().getSimpleName());
        ProblemDetail problem = ProblemDetail.forStatusAndDetail(HttpStatus.SERVICE_UNAVAILABLE,
                "Event storage is temporarily unavailable; retry later");
        problem.setType(ProblemTypes.SERVICE_UNAVAILABLE);
        return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE).header(HttpHeaders.RETRY_AFTER, "5").body(problem);
    }
}
