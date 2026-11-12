package dev.eventpipeline.service.messaging.ingestion;

import dev.eventpipeline.service.config.SqsProperties;
import io.awspring.cloud.sqs.operations.SqsTemplate;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.concurrent.CompletionException;

/**
 * Publishes a "new event" notification to the processing side. The send is asynchronous (non-blocking client), so the
 * HTTP response is never delayed by SQS. The message body only ever contains the eventId (Mongo is the
 * source of truth; the processor loads the full event from there).
 *
 * <p><strong>No distributed transaction.</strong> The Mongo insert and the SQS send are independent
 * operations. If the process crashes after the insert but before the send completes, or the send fails
 * (timeout, SQS unavailable), the initial message is lost even though the event is safely persisted in
 * status RECEIVED. {@code publish} therefore never throws and never blocks the caller: a failed send is
 * only logged, and {@code ReceivedEventRelay} re-publishes stale RECEIVED events. Duplicate sends are
 * expected and safe because processing claims are idempotent. A message can also reach the DLQ during a
 * Mongo outage while its event remains RECEIVED; the relay republishes it after Mongo recovers.
 */
@Component
@Slf4j
public class SqsEventPublisher {

    private static final String SCHEMA_VERSION_ATTRIBUTE = "schemaVersion";
    private static final String SCHEMA_VERSION = "1";

    private final SqsTemplate sqsTemplate;
    private final SqsProperties sqsProperties;

    public SqsEventPublisher(SqsTemplate sqsTemplate, SqsProperties sqsProperties) {
        this.sqsTemplate = sqsTemplate;
        this.sqsProperties = sqsProperties;
    }

    public void publish(String eventId) {
        try {
            sqsTemplate.sendAsync(to -> to
                            .queue(sqsProperties.queueName())
                            .payload(new EventMessage(eventId))
                            .header(SCHEMA_VERSION_ATTRIBUTE, SCHEMA_VERSION))
                    .whenComplete((result, error) -> {
                        if (error != null) {
                            logFailure(eventId, error);
                        } else {
                            log.debug("eventId={} messageId={} published to SQS", eventId, result.messageId());
                        }
                    });
        } catch (RuntimeException ex) {
            logFailure(eventId, ex);
        }
    }

    private void logFailure(String eventId, Throwable error) {
        Throwable cause = error instanceof CompletionException && error.getCause() != null ? error.getCause() : error;
        // WARN only, no stack trace and no payload: just enough to find the event again by id.
        log.warn("eventId={} failed to publish to SQS errorType={}", eventId, cause.getClass().getSimpleName());
    }

    /** Message body schema: only the eventId. Mongo is the source of truth for everything else. */
    private record EventMessage(String eventId) {
    }
}
