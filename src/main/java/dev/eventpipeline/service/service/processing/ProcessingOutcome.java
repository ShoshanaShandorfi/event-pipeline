package dev.eventpipeline.service.service.processing;

/** The consumer acknowledges only successful completion or an already completed duplicate. */
public enum ProcessingOutcome {
    PROCESSED,
    ALREADY_PROCESSED,
    IN_PROGRESS_ELSEWHERE,
    NOT_FOUND,
    PROCESSING_FAILED,
    ATTEMPTS_EXHAUSTED,
    ALREADY_FAILED
}
