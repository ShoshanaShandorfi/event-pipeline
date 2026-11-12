package dev.eventpipeline.service.service.ingestion;

/**
 * Outcome of an ingestion attempt, fully describing what happened so callers (the controller) never
 * need to infer status from a thrown exception:
 * <ul>
 *     <li>{@link #CREATED} - a brand new event was persisted in status RECEIVED.</li>
 *     <li>{@link #DUPLICATE} - the eventId already existed with an identical payload; the request is
 *     being idempotently replayed and the pre-existing event (with its current status) is returned.</li>
 *     <li>{@link #CONFLICT} - the eventId already existed with a different payload.</li>
 * </ul>
 */
public enum IngestionOutcome {
    CREATED,
    DUPLICATE,
    CONFLICT
}
