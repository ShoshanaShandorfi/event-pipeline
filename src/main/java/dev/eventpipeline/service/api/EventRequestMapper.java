package dev.eventpipeline.service.api;

import dev.eventpipeline.service.api.dto.CreateEventRequest;
import dev.eventpipeline.service.service.ingestion.EventCreationCommand;

/**
 * Maps web DTOs to service-layer commands so the controller stays free of field-by-field copying.
 */
public final class EventRequestMapper {

    private EventRequestMapper() {
    }

    public static EventCreationCommand toCommand(CreateEventRequest request) {
        return EventCreationCommand.builder()
                .eventId(request.eventId())
                .customerId(request.customerId())
                .type(request.type())
                .amount(request.amount())
                .currency(request.currency())
                .timestamp(request.timestamp())
                .metadata(request.metadata())
                .build();
    }
}
