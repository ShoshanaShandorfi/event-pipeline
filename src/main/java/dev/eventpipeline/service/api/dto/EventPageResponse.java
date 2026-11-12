package dev.eventpipeline.service.api.dto;

import dev.eventpipeline.service.domain.Event;
import org.springframework.data.domain.Page;

import java.util.List;

/**
 * Paginated response body for GET /api/customers/{customerId}/events.
 */
public record EventPageResponse(
        List<EventResponse> content,
        int page,
        int size,
        long totalElements,
        int totalPages
) {

    public static EventPageResponse from(Page<Event> page) {
        List<EventResponse> content = page.getContent().stream()
                .map(EventResponse::from)
                .toList();
        return new EventPageResponse(content, page.getNumber(), page.getSize(), page.getTotalElements(), page.getTotalPages());
    }
}
