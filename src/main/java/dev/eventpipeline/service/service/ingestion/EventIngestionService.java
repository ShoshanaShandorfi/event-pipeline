package dev.eventpipeline.service.service.ingestion;

import dev.eventpipeline.service.domain.Event;
import dev.eventpipeline.service.domain.EventStatus;
import dev.eventpipeline.service.repository.EventRepository;
import dev.eventpipeline.service.messaging.ingestion.SqsEventPublisher;
import lombok.extern.slf4j.Slf4j;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.stereotype.Service;

import java.time.Clock;
import java.time.Instant;

/**
 * Creates events in status RECEIVED. Idempotency relies solely on the Mongo {@code _id} uniqueness
 * constraint: we always attempt {@link EventRepository#insert}, never {@code save()}, because
 * {@code save()} performs an upsert by id and would silently overwrite an existing event (including one
 * that has already progressed past RECEIVED). We never check-then-insert, which would be racy under
 * concurrent requests for the same eventId; instead we let Mongo enforce uniqueness and react to the
 * resulting {@link DuplicateKeyException}.
 *
 * <p>This service never throws for a "different payload" duplicate: it reports {@link IngestionOutcome#CONFLICT}
 * on the returned {@link EventIngestionResult} instead, so the full outcome (created/duplicate/conflict)
 * is always visible on the result object. Translating CONFLICT into an HTTP 409 is the controller's job.
 */
@Service
@Slf4j
public class EventIngestionService {

    private final EventRepository eventRepository;
    private final Clock clock;
    private final SqsEventPublisher eventPublisher;

    public EventIngestionService(EventRepository eventRepository, Clock clock, SqsEventPublisher eventPublisher) {
        this.eventRepository = eventRepository;
        this.clock = clock;
        this.eventPublisher = eventPublisher;
    }

    public EventIngestionResult ingest(EventCreationCommand command) {
        Instant now = Instant.now(clock);

        Event event = Event.builder()
                .eventId(command.eventId())
                .customerId(command.customerId())
                .type(command.type())
                .amount(command.amount())
                .currency(command.currency())
                .timestamp(command.timestamp())
                .status(EventStatus.RECEIVED)
                .createdAt(now)
                .updatedAt(now)
                .attempts(0)
                .metadata(command.metadata())
                .build();

        try {
            eventRepository.insert(event);
            log.info("eventId={} ingested with status=RECEIVED", event.getEventId());
            // Publish only after the insert has actually completed, and only here in the service layer
            // (never from inside a Mongo callback), so the asynchronous SQS hand-off can only ever be
            // triggered by an event that is already durably persisted. See SqsEventPublisher's Javadoc for
            // the accepted no-distributed-transaction trade-off this implies.
            eventPublisher.publish(event.getEventId());
            return new EventIngestionResult(event, IngestionOutcome.CREATED);
        } catch (DuplicateKeyException ex) {
            return handleDuplicate(command);
        }
    }

    private EventIngestionResult handleDuplicate(EventCreationCommand command) {
        Event existing = eventRepository.findById(command.eventId())
                .orElseThrow(() -> new IllegalStateException(
                        "Duplicate key reported for eventId=" + command.eventId() + " but no event found"));

        if (command.matches(existing)) {
            log.info("eventId={} is a duplicate of an existing event with identical payload; replaying", command.eventId());
            return new EventIngestionResult(existing, IngestionOutcome.DUPLICATE);
        }

        log.warn("eventId={} already exists with a different payload", command.eventId());
        return new EventIngestionResult(existing, IngestionOutcome.CONFLICT);
    }

}
