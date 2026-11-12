package dev.eventpipeline.service.service.ingestion;

import dev.eventpipeline.service.domain.Event;
import java.util.Objects;

/**
 * Result of an ingestion attempt. {@code outcome} fully describes what happened (created / duplicate /
 * conflict) so the controller can decide the HTTP response purely by inspecting this object, without
 * the service needing to throw for control flow. For {@link IngestionOutcome#DUPLICATE} and
 * {@link IngestionOutcome#CONFLICT}, {@code event} is the pre-existing persisted event (reflecting its
 * current, possibly already-processed, status).
 */
public record EventIngestionResult(Event event, IngestionOutcome outcome) {

    public EventIngestionResult {
        Objects.requireNonNull(event, "event must not be null");
        Objects.requireNonNull(outcome, "outcome must not be null");
    }

    public boolean duplicate() {
        return outcome == IngestionOutcome.DUPLICATE;
    }

    public boolean conflict() {
        return outcome == IngestionOutcome.CONFLICT;
    }
}
