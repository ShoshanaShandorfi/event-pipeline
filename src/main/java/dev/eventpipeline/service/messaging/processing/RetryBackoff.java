package dev.eventpipeline.service.messaging.processing;

/** Pure, overflow-safe SQS retry delay: min(base * 2^(receiveCount - 1), cap). */
public final class RetryBackoff {

    private final int baseSeconds;
    private final int capSeconds;

    public RetryBackoff(int baseSeconds, int capSeconds) {
        if (baseSeconds < 1 || capSeconds < baseSeconds || capSeconds > 43_200) {
            throw new IllegalArgumentException("Backoff must satisfy 1 <= base <= cap <= 43200 seconds");
        }
        this.baseSeconds = baseSeconds;
        this.capSeconds = capSeconds;
    }

    public int seconds(int receiveCount) {
        long delay = baseSeconds;
        for (int i = 1; i < receiveCount && delay < capSeconds; i++) {
            delay = Math.min(delay * 2, capSeconds);
        }
        return (int) delay;
    }
}
