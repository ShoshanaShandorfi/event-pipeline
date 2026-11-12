package dev.eventpipeline.service.repository;

import dev.eventpipeline.service.domain.Event;
import dev.eventpipeline.service.domain.EventStatus;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;

import java.time.Instant;
import java.util.Optional;

public interface EventRepositoryCustom {

    Page<Event> findCustomerEvents(String customerId, Instant from, Instant to, EventStatus status, Pageable pageable);

    /**
     * Atomically claims an event for processing: matches {@code _id == eventId AND status == RECEIVED}
     * and, only if that matches, sets {@code status = PROCESSING}, {@code updatedAt = now}, and
     * increments {@code attempts} by one (as a BSON int32, never a long). A single Mongo
     * {@code findAndModify} call, never read-then-write, so concurrent/duplicate deliveries of the same
     * message can race on this call and exactly one of them will see a non-empty result.
     *
     * @return the claimed event (status now PROCESSING) if this call won the race, or empty if the
     * event was not in status RECEIVED at the time of the call (already PROCESSING/PROCESSED, or absent).
     */
    Optional<Event> claimForProcessing(String eventId, Instant now);

    /**
     * Completes only the claim identified by {@code claimedAttempts}; also removes failureReason.
     * Every worker transition matches _id, PROCESSING and attempts. Empty means ownership was lost:
     * the worker must neither overwrite the newer result nor acknowledge its SQS message.
     */
    Optional<Event> markProcessed(String eventId, int claimedAttempts, Instant now);

    Optional<Event> retryProcessing(String eventId, int claimedAttempts, String failureReason, Instant now);

    Optional<Event> markFailed(String eventId, int claimedAttempts, String failureReason, Instant now);

    /** Atomic relay lease, using the {status:1, updatedAt:1} index; the lease lasts one stale window. */
    Optional<Event> leaseStaleReceived(Instant staleBefore, Instant now);

    /** Atomic stale claim recovery, deciding RECEIVED versus FAILED inside the same Mongo update. */
    Optional<Event> recoverStaleProcessing(Instant staleBefore, Instant now, int maxAttempts);
}
