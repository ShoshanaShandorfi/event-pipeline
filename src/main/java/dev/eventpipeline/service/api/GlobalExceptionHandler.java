package dev.eventpipeline.service.api;

import com.mongodb.MongoException;
import dev.eventpipeline.service.api.exception.ApiException;
import dev.eventpipeline.service.api.exception.ProblemTypes;
import lombok.extern.slf4j.Slf4j;
import org.springframework.dao.DataAccessException;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.HttpStatusCode;
import org.springframework.http.ProblemDetail;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.context.request.WebRequest;
import org.springframework.web.method.annotation.MethodArgumentTypeMismatchException;
import org.springframework.web.servlet.mvc.method.annotation.ResponseEntityExceptionHandler;

import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Centralized exception translation producing RFC 7807 {@code ProblemDetail} responses. Internal
 * exception details (stack traces, driver-specific messages) are never included in the response body;
 * they are only written to the server logs.
 *
 * <p>Extends {@link ResponseEntityExceptionHandler} so standard Spring MVC failures (unknown route 404,
 * 405 with {@code Allow}, 415, missing parameters, ...) keep their correct status and headers instead of
 * falling through to the 500 fallback. Application errors carry a stable {@code urn:problem:*} type (see
 * {@link ProblemTypes}) so clients can branch on an identifier rather than parsing {@code detail}.
 */
@RestControllerAdvice
@Slf4j
public class GlobalExceptionHandler extends ResponseEntityExceptionHandler {

    @ExceptionHandler(ApiException.class)
    public ProblemDetail handleApiException(ApiException ex) {
        log.warn("Request rejected with {}: {}", ex.getStatus(), ex.getMessage());
        return problem(ex.getStatus(), ex.getType(), ex.getMessage());
    }

    @Override
    protected ResponseEntity<Object> handleMethodArgumentNotValid(MethodArgumentNotValidException ex,
            HttpHeaders headers, HttpStatusCode status, WebRequest request) {
        List<Map<String, String>> fieldErrors = ex.getBindingResult().getFieldErrors().stream()
                .map(fe -> Map.of(
                        "field", fe.getField(),
                        "message", Objects.requireNonNullElse(fe.getDefaultMessage(), "invalid value")))
                .toList();

        ProblemDetail problem = problem(HttpStatus.BAD_REQUEST, ProblemTypes.VALIDATION_FAILED, "Request validation failed");
        problem.setProperty("errors", fieldErrors);
        log.warn("Validation failed for request: {}", fieldErrors);
        return handleExceptionInternal(ex, problem, headers, status, request);
    }

    @Override
    protected ResponseEntity<Object> handleHttpMessageNotReadable(HttpMessageNotReadableException ex,
            HttpHeaders headers, HttpStatusCode status, WebRequest request) {
        // Parser diagnostics may contain rejected payload/metadata values; keep them out of logs.
        log.warn("Malformed request body");
        ProblemDetail problem = problem(HttpStatus.BAD_REQUEST, ProblemTypes.VALIDATION_FAILED, "Malformed JSON request body");
        return handleExceptionInternal(ex, problem, headers, status, request);
    }

    @ExceptionHandler(MethodArgumentTypeMismatchException.class)
    public ProblemDetail handleTypeMismatch(MethodArgumentTypeMismatchException ex) {
        String detail = "Parameter '" + ex.getName() + "' has an invalid value";
        log.warn(detail);
        return problem(HttpStatus.BAD_REQUEST, ProblemTypes.VALIDATION_FAILED, detail);
    }

    @ExceptionHandler({DataAccessException.class, MongoException.class})
    public ResponseEntity<ProblemDetail> handleDataAccess(RuntimeException ex) {
        // B: never publish after a failed insert. A client can retry the same id safely; an ambiguous
        // write may already have committed, in which case the relay recovers it. No driver message,
        // connection string, credentials or stack trace may cross the HTTP/log boundary here.
        log.warn("MongoDB operation unavailable; returning 503 ({})", ex.getClass().getSimpleName());
        ProblemDetail problem = problem(HttpStatus.SERVICE_UNAVAILABLE, ProblemTypes.SERVICE_UNAVAILABLE,
                "Event storage is temporarily unavailable; retry later");
        return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE).header(HttpHeaders.RETRY_AFTER, "5").body(problem);
    }

    @ExceptionHandler(Exception.class)
    public ProblemDetail handleUnexpected(Exception ex) {
        log.error("Unhandled exception while processing request errorType={}", ex.getClass().getSimpleName());
        return problem(HttpStatus.INTERNAL_SERVER_ERROR, ProblemTypes.INTERNAL_ERROR, "An unexpected error occurred");
    }

    private static ProblemDetail problem(HttpStatusCode status, URI type, String detail) {
        ProblemDetail problem = ProblemDetail.forStatusAndDetail(status, detail);
        problem.setType(type);
        return problem;
    }
}
