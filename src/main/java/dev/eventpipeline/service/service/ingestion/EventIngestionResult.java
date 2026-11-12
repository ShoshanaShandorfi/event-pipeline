package dev.eventpipeline.service.service.ingestion;

import dev.eventpipeline.service.domain.Event;

/**
 * Result of an ingestion attempt. {@code outcome} fully describes what happened (created / duplicate /
 * conflict) so the controller can decide the HTTP response purely by inspecting this object, without
 * the service needing to throw for control flow. For {@link IngestionOutcome#DUPLICATE} and
 * {@link IngestionOutcome#CONFLICT}, {@code event} is the pre-existing persisted event (reflecting its
 * current, possibly already-processed, status).
 */
public record EventIngestionResult(Event event, IngestionOutcome outcome) {

    public boolean duplicate() {
        return outcome == IngestionOutcome.DUPLICATE;
    }
}
