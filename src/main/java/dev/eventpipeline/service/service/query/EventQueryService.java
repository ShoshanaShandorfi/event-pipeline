package dev.eventpipeline.service.service.query;

import dev.eventpipeline.service.api.exception.EventNotFoundException;
import dev.eventpipeline.service.api.exception.InvalidRequestException;
import dev.eventpipeline.service.domain.Event;
import dev.eventpipeline.service.domain.EventStatus;
import dev.eventpipeline.service.repository.EventRepository;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.Arrays;
import java.util.Locale;

/**
 * Read-side entry point used by the API layer for both the single-event lookup and the paginated
 * customer event listing. All parameter parsing/validation for the listing endpoint (date formats,
 * status enum, page/size bounds) is centralized here so the controller stays thin.
 */
@Service
public class EventQueryService {

    static final int MIN_PAGE_SIZE = 1;
    static final int MAX_PAGE_SIZE = 100;

    private final EventRepository eventRepository;

    public EventQueryService(EventRepository eventRepository) {
        this.eventRepository = eventRepository;
    }

    public Event getEvent(String eventId) {
        return eventRepository.findById(eventId)
                .orElseThrow(() -> new EventNotFoundException("Event with eventId '" + eventId + "' was not found"));
    }

    public Page<Event> getCustomerEvents(String customerId, String from, String to, String status, int page, int size) {
        Instant fromInstant = parseInstant(from, "from");
        Instant toInstant = parseInstant(to, "to");
        if (fromInstant != null && toInstant != null && fromInstant.isAfter(toInstant)) {
            throw new InvalidRequestException("'from' must not be after 'to'");
        }

        EventStatus eventStatus = parseStatus(status);

        if (page < 0) {
            throw new InvalidRequestException("'page' must not be negative");
        }
        if (size < MIN_PAGE_SIZE || size > MAX_PAGE_SIZE) {
            throw new InvalidRequestException("'size' must be between " + MIN_PAGE_SIZE + " and " + MAX_PAGE_SIZE);
        }

        Pageable pageable = PageRequest.of(page, size);
        return eventRepository.findCustomerEvents(customerId, fromInstant, toInstant, eventStatus, pageable);
    }

    private Instant parseInstant(String raw, String fieldName) {
        if (raw == null || raw.isBlank()) {
            return null;
        }
        try {
            return Instant.parse(raw);
        } catch (DateTimeParseException ex) {
            throw new InvalidRequestException("'" + fieldName + "' must be a valid ISO-8601 instant");
        }
    }

    private EventStatus parseStatus(String raw) {
        if (raw == null || raw.isBlank()) {
            return null;
        }
        try {
            return EventStatus.valueOf(raw.trim().toUpperCase(Locale.ROOT));
        } catch (IllegalArgumentException ex) {
            throw new InvalidRequestException("'status' must be one of " + Arrays.toString(EventStatus.values()));
        }
    }
}
