package dev.eventpipeline.service.messaging.ingestion;

import ch.qos.logback.classic.Level;
import com.fasterxml.jackson.databind.ObjectMapper;
import dev.eventpipeline.service.config.EventCollectionInitializer;
import dev.eventpipeline.service.config.LocalStackTestcontainersConfig;
import dev.eventpipeline.service.domain.Event;
import dev.eventpipeline.service.domain.EventStatus;
import dev.eventpipeline.service.repository.EventRepository;
import dev.eventpipeline.service.testsupport.LogCapture;
import dev.eventpipeline.service.testsupport.TestEventRequests;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.DefaultApplicationArguments;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;

/**
 * Verifies the publisher stays non-blocking and never fails the HTTP request when SQS itself is slow
 * or failing, using the Mockito spy around the real {@code SqsAsyncClient} bean (a LocalStack-backed
 * client, declared in the test base class) to simulate both conditions without touching production code.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class SqsEventPublisherResilienceTest extends LocalStackTestcontainersConfig {

    @DynamicPropertySource
    static void disableConsumer(DynamicPropertyRegistry registry) {
        registry.add("aws.sqs.consumer.enabled", () -> false);
    }

    @Autowired
    private TestRestTemplate restTemplate;

    @Autowired
    private MongoTemplate mongoTemplate;

    @Autowired
    private EventRepository eventRepository;

    @Autowired
    private ObjectMapper objectMapper;

    @BeforeEach
    void setUp() throws Exception {
        mongoTemplate.getDb().drop();
        new EventCollectionInitializer().initializeEventCollection(mongoTemplate)
                .run(new DefaultApplicationArguments());
    }

    @Test
    void sendFailureStillReturns202AndLeavesEventReceivedAndLogsWarn() {
        Mockito.doReturn(CompletableFuture.failedFuture(AwsServiceException.builder().message("simulated SQS outage").build()))
                .when(sqsAsyncClient).sendMessage(any(SendMessageRequest.class));

        String eventId = "evt-pub-send-fails";
        String body = TestEventRequests.json(objectMapper, eventId, "cust-1", "10.00", "2025-01-01T00:00:00Z", null);

        try (LogCapture logs = LogCapture.attach(SqsEventPublisher.class)) {
            ResponseEntity<String> response = restTemplate.postForEntity("/api/events", jsonEntity(body), String.class);
            assertThat(response.getStatusCode()).isEqualTo(HttpStatus.ACCEPTED);

            await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertThat(logs.events())
                    .anyMatch(e -> e.getLevel() == Level.WARN && e.getFormattedMessage().contains(eventId)));
        }

        Event stored = eventRepository.findById(eventId).orElseThrow();
        assertThat(stored.getStatus()).isEqualTo(EventStatus.RECEIVED);
    }

    @Test
    void slowSendStillReturns202QuicklyAndEventuallySends() {
        CountDownLatch sendStarted = new CountDownLatch(1);
        CompletableFuture<Void> allowSend = new CompletableFuture<>();
        Mockito.doAnswer(invocation -> {
                    sendStarted.countDown();
                    // The real send only starts once the test opens the gate.
                    return allowSend.thenCompose(ignored -> {
                        try {
                            return (CompletableFuture<?>) invocation.callRealMethod();
                        } catch (Throwable ex) {
                            return CompletableFuture.failedFuture(ex);
                        }
                    });
                })
                .when(sqsAsyncClient).sendMessage(any(SendMessageRequest.class));

        String eventId = "evt-pub-slow-send";
        String body = TestEventRequests.json(objectMapper, eventId, "cust-1", "10.00", "2025-01-01T00:00:00Z", null);

        try {
            long start = System.nanoTime();
            ResponseEntity<String> response = restTemplate.postForEntity("/api/events", jsonEntity(body), String.class);
            Duration elapsed = Duration.ofNanos(System.nanoTime() - start);

            assertThat(response.getStatusCode()).isEqualTo(HttpStatus.ACCEPTED);
            // Prove the request finishes while the send is still pending.
            assertThat(elapsed).isLessThan(Duration.ofSeconds(3));
            await().atMost(Duration.ofSeconds(5)).until(() -> sendStarted.getCount() == 0);
            assertThat(mainQueueMessageCount()).isZero();
        } finally {
            allowSend.complete(null);
        }

        await().atMost(Duration.ofSeconds(10)).until(() -> mainQueueMessageCount() == 1);
    }

    private HttpEntity<String> jsonEntity(String body) {
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        return new HttpEntity<>(body, headers);
    }
}
