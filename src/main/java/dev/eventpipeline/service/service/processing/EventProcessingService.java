package dev.eventpipeline.service.service.processing;

import dev.eventpipeline.service.config.ProcessingProperties;
import dev.eventpipeline.service.domain.Event;
import dev.eventpipeline.service.domain.EventStatus;
import dev.eventpipeline.service.repository.EventRepository;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.time.Clock;
import java.time.Instant;
import java.util.Optional;

/**
 * C: duplicate deliveries compete for an atomic Mongo claim; only the winner runs the business step.
 * E: attempts is also a fencing token. A worker slower than the stale-PROCESSING threshold can still
 * execute the business step twice; fencing only prevents it overwriting a newer result. Therefore the
 * business step must itself be idempotent, including external effects.
 */
@Service
@Slf4j
public class EventProcessingService {

    private final EventRepository eventRepository;
    private final EventProcessor eventProcessor;
    private final Clock clock;
    private final ProcessingProperties properties;

    public EventProcessingService(EventRepository eventRepository, EventProcessor eventProcessor,
                                      Clock clock, ProcessingProperties properties) {
        this.eventRepository = eventRepository;
        this.eventProcessor = eventProcessor;
        this.clock = clock;
        this.properties = properties;
    }

    public ProcessingOutcome process(String eventId, String messageId) {
        // B: attempts changes only when the atomic claim succeeds. Repository exceptions propagate
        // to the consumer, which retains the message and backs off without inventing an attempt.
        Optional<Event> claimed = eventRepository.claimForProcessing(eventId, Instant.now(clock));
        return claimed.isPresent() ? runClaimedEvent(claimed.get(), messageId) : handleClaimMiss(eventId, messageId);
    }

    private ProcessingOutcome runClaimedEvent(Event event, String messageId) {
        String eventId = event.getEventId();
        int claimedAttempts = event.getAttempts();
        try {
            eventProcessor.process(event);
        } catch (Exception ex) {
            return handleBusinessFailure(event, messageId, ex);
        }

        if (eventRepository.markProcessed(eventId, claimedAttempts, Instant.now(clock)).isEmpty()) {
            return handleLostClaim(eventId, messageId);
        }
        log.info("eventId={} messageId={} processed successfully", eventId, messageId);
        return ProcessingOutcome.PROCESSED;
    }

    private ProcessingOutcome handleBusinessFailure(Event event, String messageId, Exception ex) {
        // A: business failures consume a successfully claimed attempt, then retry with SQS backoff.
        // Repository operations deliberately sit outside the business-step try boundary so a
        // Mongo transition failure never gets mistaken for another business failure.
        String eventId = event.getEventId();
        int claimedAttempts = event.getAttempts();
        String reason = FailureReason.from(ex, event);
        boolean exhausted = claimedAttempts >= properties.maxAttempts();
        Optional<Event> transitioned = exhausted
                ? eventRepository.markFailed(eventId, claimedAttempts, reason, Instant.now(clock))
                : eventRepository.retryProcessing(eventId, claimedAttempts, reason, Instant.now(clock));
        if (transitioned.isEmpty()) {
            return handleLostClaim(eventId, messageId);
        }
        log.warn("eventId={} messageId={} processing failed at attempt={}; status={}",
                eventId, messageId, claimedAttempts, exhausted ? EventStatus.FAILED : EventStatus.RECEIVED);
        // D: never acknowledge FAILED. Observed with LocalStack 4.14: receives 1..maxReceiveCount
        // return the message. After the final visibility timeout, the next ReceiveMessage moves
        // it to the DLQ and returns no message to this worker (SqsRedriveIntegrationTest).
        return exhausted ? ProcessingOutcome.ATTEMPTS_EXHAUSTED : ProcessingOutcome.PROCESSING_FAILED;
    }

    private ProcessingOutcome handleClaimMiss(String eventId, String messageId) {
        Optional<Event> existing = eventRepository.findById(eventId);
        if (existing.isEmpty()) {
            log.warn("eventId={} messageId={} event not found; retaining message for redrive", eventId, messageId);
            return ProcessingOutcome.NOT_FOUND;
        }
        EventStatus status = existing.get().getStatus();
        if (status == EventStatus.PROCESSED) {
            log.info("eventId={} messageId={} already processed; acknowledging duplicate", eventId, messageId);
            return ProcessingOutcome.ALREADY_PROCESSED;
        }
        if (status == EventStatus.FAILED) {
            // FAILED is terminal in the automatic flow. An operator may deliberately reset it to
            // RECEIVED for relay publication; inspection/replay/removal of its DLQ copy is operational.
            log.warn("eventId={} messageId={} already FAILED; retaining message for redrive", eventId, messageId);
            return ProcessingOutcome.ALREADY_FAILED;
        }
        log.info("eventId={} messageId={} status={}; waiting for worker or recovery", eventId, messageId, status);
        return ProcessingOutcome.IN_PROGRESS_ELSEWHERE;
    }

    /** A newer worker owns the event now (our fenced update matched nothing); leave it to them. */
    private ProcessingOutcome handleLostClaim(String eventId, String messageId) {
        log.warn("eventId={} messageId={} processing ownership lost; retaining message", eventId, messageId);
        return ProcessingOutcome.IN_PROGRESS_ELSEWHERE;
    }
}
