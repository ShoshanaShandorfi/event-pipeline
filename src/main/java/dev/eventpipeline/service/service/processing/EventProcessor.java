package dev.eventpipeline.service.service.processing;

import dev.eventpipeline.service.domain.Event;
import org.springframework.stereotype.Component;

/**
 * The business step applied to a claimed event. Currently a placeholder with no external side effects;
 * real processing logic goes in {@link #process(Event)}.
 *
 * <p>It must be idempotent: a crash, a failed completion write, or execution beyond the recovery
 * threshold can cause it to run again. Fencing prevents an old worker from overwriting newer Mongo
 * state, but cannot undo external business effects. Exception messages must describe the failure
 * without embedding payload, metadata, or secrets; persisted reasons additionally redact event values
 * and common sensitive message formats.
 */
@Component
public class EventProcessor {

    public void process(Event event) {
        // Deliberately no external side effects in the placeholder business step.
    }
}
