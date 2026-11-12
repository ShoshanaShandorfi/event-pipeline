package dev.eventpipeline.service.messaging.processing;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

class RetryBackoffTest {

    @ParameterizedTest(name = "base={0} cap={1} receiveCount={2} -> {3}s")
    @CsvSource({
            "5, 60, 1, 5", "5, 60, 2, 10", "5, 60, 3, 20", "5, 60, 4, 40", "5, 60, 5, 60",
            "5, 60, 2147483647, 60",   // doubles until the cap, never overflows
            "5, 60, 0, 5", "5, 60, -10, 5",   // invalid receive count falls back to base
            "5, 5, 100, 5"             // base == cap is stable
    })
    void backoffDoublesUntilCap(int base, int cap, int receiveCount, long expected) {
        assertThat(new RetryBackoff(base, cap).seconds(receiveCount)).isEqualTo(expected);
    }

    @ParameterizedTest(name = "base={0} cap={1} rejected")
    @CsvSource({"0, 60", "10, 5", "1, 43201"})
    void rejectsInvalidSqsVisibilitySettings(int base, int cap) {
        assertThatIllegalArgumentException().isThrownBy(() -> new RetryBackoff(base, cap));
    }
}
