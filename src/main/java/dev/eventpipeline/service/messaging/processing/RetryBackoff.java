package dev.eventpipeline.service.messaging.processing;

import java.util.concurrent.ThreadLocalRandom;
import java.util.function.IntUnaryOperator;

/**
 * Pure, overflow-safe SQS retry delay with "equal jitter": the exponential ceiling
 * {@code min(base * 2^(receiveCount - 1), cap)} is halved, and a random amount up to the other half is
 * added. The result is in {@code [ceiling - ceiling/2, ceiling]} and never below 1 second, so messages
 * that failed together (e.g. during a Mongo outage) do not all become visible at the same instant.
 */
public final class RetryBackoff {

    private final int baseSeconds;
    private final int capSeconds;
    /** Returns a uniformly distributed value in {@code [0, bound)}. */
    private final IntUnaryOperator random;

    public RetryBackoff(int baseSeconds, int capSeconds) {
        this(baseSeconds, capSeconds, bound -> ThreadLocalRandom.current().nextInt(bound));
    }

    RetryBackoff(int baseSeconds, int capSeconds, IntUnaryOperator random) {
        if (baseSeconds < 1 || capSeconds < baseSeconds || capSeconds > 43_200) {
            throw new IllegalArgumentException("Backoff must satisfy 1 <= base <= cap <= 43200 seconds");
        }
        this.baseSeconds = baseSeconds;
        this.capSeconds = capSeconds;
        this.random = random;
    }

    /** Deterministic upper bound of the delay for this receive count. */
    public int ceilingSeconds(int receiveCount) {
        long delay = baseSeconds;
        for (int i = 1; i < receiveCount && delay < capSeconds; i++) {
            delay = Math.min(delay * 2, capSeconds);
        }
        return (int) delay;
    }

    /** Jittered delay to use as the SQS visibility timeout. */
    public int seconds(int receiveCount) {
        int ceiling = ceilingSeconds(receiveCount);
        int half = ceiling / 2;
        return (ceiling - half) + random.applyAsInt(half + 1);
    }
}
