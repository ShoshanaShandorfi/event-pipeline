package dev.eventpipeline.service.testsupport;

import dev.eventpipeline.service.config.LocalStackTestcontainersConfig;
import org.springframework.boot.test.context.SpringBootTest;

/**
 * One cached application context for API/publisher tests that do not run background processing.
 * Keep common properties here: distinct per-class DynamicPropertySource methods create different
 * Spring cache keys even when their supplied values are identical.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, properties = {
        "aws.sqs.consumer.enabled=false", "pipeline.relay.enabled=false", "pipeline.recovery.enabled=false"
})
public abstract class IngestionIntegrationTestSupport extends LocalStackTestcontainersConfig {
}
