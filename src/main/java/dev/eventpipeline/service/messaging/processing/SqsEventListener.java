package dev.eventpipeline.service.messaging.processing;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.MongoException;
import dev.eventpipeline.service.config.ProcessingProperties;
import dev.eventpipeline.service.service.processing.EventProcessingService;
import dev.eventpipeline.service.service.processing.ProcessingOutcome;
import io.awspring.cloud.sqs.annotation.SqsListener;
import io.awspring.cloud.sqs.listener.SqsHeaders;
import io.awspring.cloud.sqs.listener.Visibility;
import io.awspring.cloud.sqs.listener.acknowledgement.Acknowledgement;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.dao.DataAccessException;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;
import org.springframework.stereotype.Component;

/**
 * Spring Cloud AWS {@link SqsListener} for the main queue. Polling, concurrency, graceful shutdown and
 * queue resolution are handled by the listener container configured in {@link SqsConsumerConfig}; this
 * class only decides what to do with each message. The container runs in MANUAL acknowledgement mode:
 * a message is deleted from the queue ONLY after it was handled successfully or recognized as an
 * already-processed duplicate; any other outcome leaves it undeleted (with a backed-off visibility
 * timeout) so SQS redelivers it, and the redrive policy eventually moves it to the DLQ. Its
 * maxReceiveCount must match the configured maximum processing attempts.
 *
 * <p>Entirely absent from the context when {@code aws.sqs.consumer.enabled=false} (see
 * {@link SqsConsumerConfig}), so API-only tests never start a background poller.
 */
@Component
@ConditionalOnProperty(prefix = "aws.sqs.consumer", name = "enabled", havingValue = "true", matchIfMissing = true)
@Slf4j
public class SqsEventListener {

    private final EventProcessingService processingService;
    private final ObjectMapper objectMapper;
    private final RetryBackoff retryBackoff;

    public SqsEventListener(EventProcessingService processingService,
                            ObjectMapper objectMapper,
                            ProcessingProperties processingProperties) {
        this.processingService = processingService;
        this.objectMapper = objectMapper;
        this.retryBackoff = new RetryBackoff(processingProperties.retryBackoffBaseSeconds(),
                processingProperties.retryBackoffCapSeconds());
    }

    @SqsListener("${aws.sqs.queue-name}")
    public void onMessage(Message<String> message, Acknowledgement acknowledgement, Visibility visibility) {
        MessageHeaders headers = message.getHeaders();
        String messageId = String.valueOf(headers.getId());
        int receiveCount = receiveCount(headers);

        String eventId = extractEventId(message.getPayload());
        if (eventId == null) {
            log.warn("eventId=unavailable messageId={} malformed message; retaining for redrive", messageId);
            changeVisibility(visibility, "unavailable", messageId, retryBackoff.seconds(receiveCount));
            return;
        }

        try {
            ProcessingOutcome outcome = processingService.process(eventId, messageId);
            if (outcome == ProcessingOutcome.PROCESSED || outcome == ProcessingOutcome.ALREADY_PROCESSED) {
                acknowledge(acknowledgement, eventId, messageId);
            } else {
                int backoffCount = outcome == ProcessingOutcome.IN_PROGRESS_ELSEWHERE ? 1 : receiveCount;
                changeVisibility(visibility, eventId, messageId, retryBackoff.seconds(backoffCount));
            }
        } catch (DataAccessException | MongoException ex) {
            // B: failed Mongo calls never acknowledge SQS. Only a successful atomic claim increments
            // attempts; read/transition failures cannot consume another attempt. Even if repeated
            // receives move a still-RECEIVED event to the DLQ while Mongo is down, relay publication
            // after recovery is safe: Mongo remains the durable source and duplicate claims are atomic.
            log.warn("eventId={} messageId={} Mongo operation unavailable; retaining message and backing off",
                    eventId, messageId);
            changeVisibility(visibility, eventId, messageId, retryBackoff.seconds(receiveCount));
        } catch (Exception ex) {
            log.warn("eventId={} messageId={} unexpected processing error; retaining message and backing off",
                    eventId, messageId);
            changeVisibility(visibility, eventId, messageId, retryBackoff.seconds(receiveCount));
        }
    }

    private int receiveCount(MessageHeaders headers) {
        try {
            Object value = headers.get(SqsHeaders.MessageSystemAttributes.SQS_APPROXIMATE_RECEIVE_COUNT);
            return value == null ? 1 : Math.max(1, Integer.parseInt(value.toString()));
        } catch (NumberFormatException ex) {
            return 1;
        }
    }

    private void changeVisibility(Visibility visibility, String eventId, String messageId, int seconds) {
        try {
            visibility.changeTo(seconds);
        } catch (Exception ex) {
            // The original visibility timeout still protects redelivery if SQS is temporarily down.
            log.warn("eventId={} messageId={} could not change visibility; retaining message", eventId, messageId);
        }
    }

    private String extractEventId(String body) {
        try {
            JsonNode node = objectMapper.readTree(body);
            String eventId = node.path("eventId").asText(null);
            return (eventId == null || eventId.isBlank()) ? null : eventId;
        } catch (Exception ex) {
            return null;
        }
    }

    private void acknowledge(Acknowledgement acknowledgement, String eventId, String messageId) {
        try {
            acknowledgement.acknowledge();
        } catch (Exception ex) {
            log.warn("eventId={} messageId={} failed to delete message after successful processing",
                    eventId, messageId);
        }
    }
}
