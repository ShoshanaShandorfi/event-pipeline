package dev.eventpipeline.service.api;

import dev.eventpipeline.service.api.dto.EventPageResponse;
import dev.eventpipeline.service.domain.Event;
import dev.eventpipeline.service.service.query.EventQueryService;
import org.springframework.data.domain.Page;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * Thin controller for listing a customer's events. Query parameters are passed through to the service
 * layer as raw strings so that parsing/validation (ISO-8601 instants, status enum, page/size bounds)
 * is centralized in one place and consistently reported as 400 Bad Request.
 */
@RestController
@RequestMapping("/api/customers/{customerId}/events")
public class CustomerEventController {

    private final EventQueryService eventQueryService;

    public CustomerEventController(EventQueryService eventQueryService) {
        this.eventQueryService = eventQueryService;
    }

    @GetMapping
    public EventPageResponse getCustomerEvents(
            @PathVariable String customerId,
            @RequestParam(required = false) String from,
            @RequestParam(required = false) String to,
            @RequestParam(required = false) String status,
            @RequestParam(defaultValue = "0") int page,
            @RequestParam(defaultValue = "20") int size) {

        Page<Event> result = eventQueryService.getCustomerEvents(customerId, from, to, status, page, size);
        return EventPageResponse.from(result);
    }
}
