package dev.eventpipeline.service.config;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatIllegalStateException;

class PipelineSettingsValidatorTest {

    private static SqsConsumerProperties consumer(boolean enabled, int visibilityTimeoutSeconds) {
        return new SqsConsumerProperties(enabled, 10, 20, visibilityTimeoutSeconds, 4, 25);
    }

    private static RecoveryProperties recovery(boolean enabled, int staleThresholdSeconds) {
        return new RecoveryProperties(enabled, 10, staleThresholdSeconds);
    }

    @Test
    void acceptsThresholdLargerThanVisibilityTimeout() {
        assertThatCode(() -> new PipelineSettingsValidator(consumer(true, 30), recovery(true, 300)))
                .doesNotThrowAnyException();
    }

    @Test
    void rejectsThresholdNotLargerThanVisibilityTimeout() {
        assertThatIllegalStateException()
                .isThrownBy(() -> new PipelineSettingsValidator(consumer(true, 30), recovery(true, 30)))
                .withMessageContaining("stale-processing-threshold-seconds");
    }

    @Test
    void skipsCheckWhenEitherSideIsDisabled() {
        assertThatCode(() -> new PipelineSettingsValidator(consumer(false, 30), recovery(true, 5)))
                .doesNotThrowAnyException();
        assertThatCode(() -> new PipelineSettingsValidator(consumer(true, 30), recovery(false, 5)))
                .doesNotThrowAnyException();
    }
}
