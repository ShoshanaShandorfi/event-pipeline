package dev.eventpipeline.service.config;

import org.junit.jupiter.api.BeforeEach;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.context.bean.override.mockito.MockitoSpyBean;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsAsyncClient;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.CreateQueueRequest;
import software.amazon.awssdk.services.sqs.model.GetQueueAttributesRequest;
import software.amazon.awssdk.services.sqs.model.GetQueueAttributesResponse;
import software.amazon.awssdk.services.sqs.model.PurgeQueueRequest;
import software.amazon.awssdk.services.sqs.model.QueueAttributeName;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;

import java.time.Duration;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

import static org.awaitility.Awaitility.await;

/**
 * Testcontainers base for tests that need a real SQS (via LocalStack, pinned to the same image used in
 * {@code docker-compose.yml}) in addition to MongoDB. Creates the main queue, the DLQ, and the redrive
 * policy itself in test setup -- deliberately independent of the local {@code .env} file and of
 * {@code localstack/init/ready.d}, so these tests never depend on the local dev stack.
 */
public abstract class LocalStackTestcontainersConfig extends MongoTestcontainersConfig {

    public static final String QUEUE_NAME = "test-events";
    public static final String DLQ_NAME = "test-events-dlq";
    protected static final int MAX_RECEIVE_COUNT = 3;

    private static final LocalStackContainer LOCALSTACK =
            new LocalStackContainer(DockerImageName.parse("localstack/localstack:4.14"))
                    .withServices(LocalStackContainer.Service.SQS);

    private static volatile SqsClient adminSqsClient;
    private static volatile String mainQueueUrl;
    private static volatile String dlqUrl;

    /** Publisher sends that have been started but not yet completed, across all cached Spring contexts. */
    private static final Set<CompletableFuture<?>> IN_FLIGHT_SENDS = ConcurrentHashMap.newKeySet();

    /** Spy on the shared async client, only used to track in-flight publisher sends (see below). */
    @MockitoSpyBean
    protected SqsAsyncClient sqsAsyncClient;

    @DynamicPropertySource
    static void sqsProperties(DynamicPropertyRegistry registry) {
        ensureStarted();

        registry.add("aws.region", LOCALSTACK::getRegion);
        registry.add("aws.sqs.endpoint", () -> LOCALSTACK.getEndpointOverride(LocalStackContainer.Service.SQS).toString());
        registry.add("aws.sqs.queue-name", () -> QUEUE_NAME);
        registry.add("aws.sqs.dlq-name", () -> DLQ_NAME);
    }

    /**
     * Idempotently starts LocalStack and creates the queues. Decoupled from
     * {@link DynamicPropertySource} so plain (non-Spring) tests can reuse the same shared container by
     * calling this directly, e.g. when constructing an {@code SqsEventPublisher} by hand to control its
     * lifecycle precisely.
     */
    public static synchronized void ensureStarted() {
        if (LOCALSTACK.isRunning()) {
            return;
        }
        LOCALSTACK.start();
        // Dummy, non-secret LocalStack test credentials (not used against real AWS, where the
        // default credentials provider chain picks up the task/instance IAM role instead). Set as
        // system properties so the application's DefaultCredentialsProvider resolves them the same
        // way the real application resolves AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY env vars.
        System.setProperty("aws.accessKeyId", LOCALSTACK.getAccessKey());
        System.setProperty("aws.secretAccessKey", LOCALSTACK.getSecretKey());
        createQueues();
    }

    public static String region() {
        return LOCALSTACK.getRegion();
    }

    public static String endpoint() {
        return LOCALSTACK.getEndpointOverride(LocalStackContainer.Service.SQS).toString();
    }

    private static void createQueues() {
        SqsClient client = SqsClient.builder()
                .endpointOverride(LOCALSTACK.getEndpointOverride(LocalStackContainer.Service.SQS))
                .region(Region.of(LOCALSTACK.getRegion()))
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create(LOCALSTACK.getAccessKey(), LOCALSTACK.getSecretKey())))
                .build();

        String dlqUrlLocal = client.createQueue(CreateQueueRequest.builder().queueName(DLQ_NAME).build()).queueUrl();
        String dlqArn = client.getQueueAttributes(GetQueueAttributesRequest.builder()
                        .queueUrl(dlqUrlLocal)
                        .attributeNames(QueueAttributeName.QUEUE_ARN)
                        .build())
                .attributes().get(QueueAttributeName.QUEUE_ARN);

        String redrivePolicy = "{\"deadLetterTargetArn\":\"" + dlqArn + "\",\"maxReceiveCount\":\"" + MAX_RECEIVE_COUNT + "\"}";
        String queueUrlLocal = client.createQueue(CreateQueueRequest.builder()
                        .queueName(QUEUE_NAME)
                        .attributes(Map.of(QueueAttributeName.REDRIVE_POLICY, redrivePolicy))
                        .build())
                .queueUrl();

        adminSqsClient = client;
        mainQueueUrl = queueUrlLocal;
        dlqUrl = dlqUrlLocal;
    }

    /** Keeps tests independent from one another even though the container/queues are shared per JVM. */
    @BeforeEach
    void purgeQueuesBeforeEachTest() {
        // Publishing is asynchronous. Track every send the real client starts, and finish them all
        // before purging, so a late send from a previous test cannot leak into the next one.
        // (Plain tests without a Spring context have no client to track.)
        if (sqsAsyncClient != null) {
            Mockito.doAnswer(invocation -> {
                CompletableFuture<?> send = (CompletableFuture<?>) invocation.callRealMethod();
                IN_FLIGHT_SENDS.add(send);
                send.whenComplete((result, error) -> IN_FLIGHT_SENDS.remove(send));
                return send;
            }).when(sqsAsyncClient).sendMessage(ArgumentMatchers.any(SendMessageRequest.class));
        }

        awaitPublisherIdle();
        adminSqsClient.purgeQueue(PurgeQueueRequest.builder().queueUrl(mainQueueUrl).build());
        adminSqsClient.purgeQueue(PurgeQueueRequest.builder().queueUrl(dlqUrl).build());
    }

    protected void awaitPublisherIdle() {
        await().pollInterval(Duration.ofMillis(25)).atMost(Duration.ofSeconds(10)).until(IN_FLIGHT_SENDS::isEmpty);
    }

    public static SqsClient adminSqsClient() {
        return adminSqsClient;
    }

    public static String mainQueueUrl() {
        return mainQueueUrl;
    }

    public static String dlqUrl() {
        return dlqUrl;
    }

    public static long mainQueueMessageCount() {
        return approxMessageCount(mainQueueUrl);
    }

    public static long dlqMessageCount() {
        return approxMessageCount(dlqUrl);
    }

    private static long approxMessageCount(String queueUrl) {
        GetQueueAttributesResponse attrs = adminSqsClient.getQueueAttributes(GetQueueAttributesRequest.builder()
                .queueUrl(queueUrl)
                .attributeNames(QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES,
                        QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES_NOT_VISIBLE)
                .build());
        long visible = Long.parseLong(attrs.attributes().getOrDefault(QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES, "0"));
        long inFlight = Long.parseLong(attrs.attributes().getOrDefault(QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES_NOT_VISIBLE, "0"));
        return visible + inFlight;
    }
}
