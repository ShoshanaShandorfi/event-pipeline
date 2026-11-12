package dev.eventpipeline.service.api;

import dev.eventpipeline.service.api.exception.ProblemTypes;
import dev.eventpipeline.service.domain.Event;
import dev.eventpipeline.service.domain.EventType;
import dev.eventpipeline.service.domain.EventStatus;
import dev.eventpipeline.service.service.ingestion.EventIngestionResult;
import dev.eventpipeline.service.service.ingestion.EventIngestionService;
import dev.eventpipeline.service.service.ingestion.IngestionOutcome;
import dev.eventpipeline.service.service.query.EventQueryService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.header;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/** Controller routing, validation and advice with fresh service mocks, without a Boot context. */
class EventControllerTest {

    private MockMvc mockMvc;

    private EventIngestionService eventIngestionService;

    private EventQueryService eventQueryService;

    @BeforeEach
    void setUp() {
        eventIngestionService = mock(EventIngestionService.class);
        eventQueryService = mock(EventQueryService.class);
        mockMvc = MockMvcBuilders.standaloneSetup(new EventController(eventIngestionService, eventQueryService))
                .setControllerAdvice(new GlobalExceptionHandler())
                .build();
    }

    private static Event sampleEvent(String eventId, EventStatus status) {
        Instant now = Instant.parse("2025-01-01T00:00:00Z");
        return new Event(eventId, "cust-1", EventType.PAYMENT, new BigDecimal("10.00"), "USD",
                now, status, now, now, null, 0, null, Map.of());
    }

    private String validRequestJson() {
        return """
                {
                  "eventId": "evt-1",
                  "customerId": "cust-1",
                  "type": "PAYMENT",
                  "amount": 10.00,
                  "currency": "USD",
                  "timestamp": "2025-01-01T00:00:00Z"
                }
                """;
    }

    @Test
    void createEvent_newEventReturns202WithLocationAndBody() throws Exception {
        when(eventIngestionService.ingest(any())).thenReturn(new EventIngestionResult(sampleEvent("evt-1", EventStatus.RECEIVED), IngestionOutcome.CREATED));

        mockMvc.perform(post("/api/events").contentType(MediaType.APPLICATION_JSON).content(validRequestJson()))
                .andExpect(status().isAccepted())
                .andExpect(header().string("Location", "/api/events/evt-1"))
                .andExpect(header().doesNotExist("Idempotent-Replayed"))
                .andExpect(jsonPath("$.eventId").value("evt-1"))
                .andExpect(jsonPath("$.status").value("RECEIVED"))
                .andExpect(jsonPath("$.duplicate").value(false));
    }

    @Test
    void createEvent_duplicateReturns202WithReplayedHeaderAndCurrentStatus() throws Exception {
        when(eventIngestionService.ingest(any())).thenReturn(new EventIngestionResult(sampleEvent("evt-1", EventStatus.PROCESSED), IngestionOutcome.DUPLICATE));

        mockMvc.perform(post("/api/events").contentType(MediaType.APPLICATION_JSON).content(validRequestJson()))
                .andExpect(status().isAccepted())
                .andExpect(header().string("Idempotent-Replayed", "true"))
                .andExpect(header().string("Location", "/api/events/evt-1"))
                .andExpect(jsonPath("$.status").value("PROCESSED"))
                .andExpect(jsonPath("$.duplicate").value(true));
    }

    @Test
    void createEvent_conflictingPayloadReturns409() throws Exception {
        when(eventIngestionService.ingest(any())).thenReturn(new EventIngestionResult(sampleEvent("evt-1", EventStatus.RECEIVED), IngestionOutcome.CONFLICT));

        mockMvc.perform(post("/api/events").contentType(MediaType.APPLICATION_JSON).content(validRequestJson()))
                .andExpect(status().isConflict())
                .andExpect(jsonPath("$.status").value(409))
                .andExpect(jsonPath("$.type").value(ProblemTypes.EVENT_PAYLOAD_CONFLICT.toString()));
    }

    @ParameterizedTest
    @ValueSource(strings = {
            // blank ids, negative amount, lower-case currency
            "{\"eventId\":\"\",\"customerId\":\"\",\"type\":\"PAYMENT\",\"amount\":-1,\"currency\":\"usd\",\"timestamp\":\"2025-01-01T00:00:00Z\"}",
            // unknown event type
            "{\"eventId\":\"evt-1\",\"customerId\":\"c\",\"type\":\"NOT_A_TYPE\",\"amount\":10,\"currency\":\"USD\",\"timestamp\":\"2025-01-01T00:00:00Z\"}",
            // malformed JSON
            "{not-json"})
    void createEvent_invalidBodyReturns400ValidationProblem(String body) throws Exception {
        mockMvc.perform(post("/api/events").contentType(MediaType.APPLICATION_JSON).content(body))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.type").value(ProblemTypes.VALIDATION_FAILED.toString()));
    }
}
