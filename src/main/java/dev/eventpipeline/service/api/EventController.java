package dev.eventpipeline.service.api;

import dev.eventpipeline.service.api.dto.CreateEventRequest;
import dev.eventpipeline.service.api.dto.EventAcceptedResponse;
import dev.eventpipeline.service.api.dto.EventResponse;
import dev.eventpipeline.service.api.exception.ApiException;
import dev.eventpipeline.service.domain.Event;
import dev.eventpipeline.service.service.ingestion.EventCreationCommand;
import dev.eventpipeline.service.service.ingestion.EventIngestionResult;
import dev.eventpipeline.service.service.ingestion.EventIngestionService;
import dev.eventpipeline.service.service.query.EventQueryService;
import jakarta.validation.Valid;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.util.UriComponentsBuilder;

import java.net.URI;

/**
 * Thin controller for single-event endpoints. All business logic (idempotent create, lookups) lives in
 * the service layer so this class only translates between HTTP and the service API.
 */
@RestController
@RequestMapping("/api/events")
@Slf4j
public class EventController {

    private static final String IDEMPOTENT_REPLAYED_HEADER = "Idempotent-Replayed";

    private final EventIngestionService eventIngestionService;
    private final EventQueryService eventQueryService;

    public EventController(EventIngestionService eventIngestionService, EventQueryService eventQueryService) {
        this.eventIngestionService = eventIngestionService;
        this.eventQueryService = eventQueryService;
    }

    @PostMapping
    public ResponseEntity<EventAcceptedResponse> createEvent(@Valid @RequestBody CreateEventRequest request) {
        EventCreationCommand command = EventRequestMapper.toCommand(request);

        EventIngestionResult result = eventIngestionService.ingest(command);
        Event event = result.event();

        // The service never throws for a payload conflict; it reports it on the result object, so the
        // HTTP-status decision (and the 409 ProblemDetail body) stays entirely at this edge.
        if (result.conflict()) {
            log.warn("eventId={} rejected: already exists with a different payload", event.getEventId());
            throw ApiException.conflict("Event with eventId '" + event.getEventId() + "' already exists with a different payload");
        }

        log.info("eventId={} accepted (duplicate={})", event.getEventId(), result.duplicate());

        EventAcceptedResponse body = new EventAcceptedResponse(event.getEventId(), event.getStatus(), result.duplicate());
        URI location = UriComponentsBuilder.fromPath("/api/events/{eventId}")
                .encode()
                .buildAndExpand(event.getEventId())
                .toUri();

        ResponseEntity.BodyBuilder responseBuilder = ResponseEntity.accepted().location(location);
        if (result.duplicate()) {
            responseBuilder = responseBuilder.header(IDEMPOTENT_REPLAYED_HEADER, "true");
        }
        return responseBuilder.body(body);
    }

    @GetMapping("/{eventId}")
    public EventResponse getEvent(@PathVariable String eventId) {
        Event event = eventQueryService.getEvent(eventId);
        return EventResponse.from(event);
    }
}
