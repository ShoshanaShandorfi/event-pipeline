package dev.eventpipeline.service.service.processing;

import dev.eventpipeline.service.domain.Event;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.math.BigDecimal;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class FailureReasonTest {

    @Test
    void storesClassAndAtMostTwoHundredMessageCharactersWithoutControlCharacters() {
        String reason = FailureReason.from(new IllegalStateException("business failure\r\n\t" + "x".repeat(300)), new Event());

        assertThat(reason).startsWith("java.lang.IllegalStateException: business failure   ")
                .hasSize("java.lang.IllegalStateException: ".length() + 200)
                .doesNotContain("\r", "\n", "\t");
    }

    @Test
    void redactsKnownEventAndMetadataValuesWithoutSerializingThem() {
        Event event = new Event();
        event.setAmount(new BigDecimal("123.45"));
        event.setCustomerId("customer-sensitive");
        event.setMetadata(Map.of("sensitive-key", "confidential-value\nsecond-line"));

        String reason = FailureReason.from(new RuntimeException("cannot handle customer-sensitive 123.45 sensitive-key confidential-value\nsecond-line"), event);

        assertThat(reason).isEqualTo("java.lang.RuntimeException: cannot handle [redacted] [redacted] [redacted] [redacted]");
    }

    @ParameterizedTest
    @ValueSource(strings = {"payload={anything}", "metadata private-value", "password=hunter2",
            "authorization Bearer abc", "connection https://name:pass@example.test", "token abc"})
    void suppressesStructuredDataAndSecretBearingMessages(String diagnostic) {
        assertThat(FailureReason.from(new RuntimeException(diagnostic), new Event()))
                .isEqualTo("java.lang.RuntimeException: [redacted sensitive diagnostic]");
    }
}
