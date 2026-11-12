package dev.eventpipeline.service.messaging.processing;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

class RetryBackoffTest {

    @ParameterizedTest(name = "base={0} cap={1} count={2} -> ceiling {3}s, jittered [{4}, {5}]")
    @CsvSource({
            "5, 60, 1, 5, 3, 5",                 // first attempt uses base
            "5, 60, 3, 20, 10, 20",              // doubles each attempt
            "5, 60, 5, 60, 30, 60",              // reaches the cap
            "5, 60, 2147483647, 60, 30, 60",     // never overflows
            "5, 60, -10, 5, 3, 5",               // invalid receive count falls back to base
            "1, 1, 50, 1, 1, 1"                  // never below 1s (visibility 0 = instant retry)
    })
    void ceilingAndJitterBounds(int base, int cap, int count, int ceiling, int min, int max) {
        assertThat(new RetryBackoff(base, cap).ceilingSeconds(count)).isEqualTo(ceiling);
        assertThat(new RetryBackoff(base, cap, bound -> 0).seconds(count)).isEqualTo(min);
        assertThat(new RetryBackoff(base, cap, bound -> bound - 1).seconds(count)).isEqualTo(max);
    }

    @ParameterizedTest(name = "default random stays in range (count {0})")
    @CsvSource({"1", "10"})
    void defaultRandomIsBounded(int count) {
        RetryBackoff backoff = new RetryBackoff(5, 60);
        int ceiling = backoff.ceilingSeconds(count);
        for (int i = 0; i < 200; i++) {
            assertThat(backoff.seconds(count)).isBetween(ceiling - ceiling / 2, ceiling);
        }
    }

    @ParameterizedTest(name = "base={0} cap={1} rejected")
    @CsvSource({"0, 60", "10, 5", "1, 43201"})
    void rejectsInvalidSqsVisibilitySettings(int base, int cap) {
        assertThatIllegalArgumentException().isThrownBy(() -> new RetryBackoff(base, cap));
    }
}
