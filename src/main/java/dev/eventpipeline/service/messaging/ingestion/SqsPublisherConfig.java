package dev.eventpipeline.service.messaging.ingestion;

import io.awspring.cloud.sqs.listener.QueueNotFoundStrategy;
import io.awspring.cloud.sqs.operations.SqsTemplate;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import software.amazon.awssdk.services.sqs.SqsAsyncClient;

@Configuration
public class SqsPublisherConfig {

    @Bean
    public SqsTemplate sqsTemplate(SqsAsyncClient sqsAsyncClient) {
        return SqsTemplate.builder()
                .sqsAsyncClient(sqsAsyncClient)
                // Never create a queue implicitly; a missing queue is a deployment error.
                .configure(options -> options.queueNotFoundStrategy(QueueNotFoundStrategy.FAIL))
                // The body is just {"eventId": ...}; no Java class name in the message attributes.
                .configureDefaultConverter(converter -> converter.doNotSendPayloadTypeHeader())
                .build();
    }
}
