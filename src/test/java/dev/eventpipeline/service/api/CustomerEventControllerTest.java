package dev.eventpipeline.service.api;

import dev.eventpipeline.service.api.exception.ApiException;
import dev.eventpipeline.service.api.exception.ProblemTypes;
import dev.eventpipeline.service.domain.Event;
import dev.eventpipeline.service.domain.EventType;
import dev.eventpipeline.service.domain.EventStatus;
import dev.eventpipeline.service.service.query.EventQueryService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.PageRequest;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/** Request binding and advice with a fresh query mock, without a Boot context. */
class CustomerEventControllerTest {

    private MockMvc mockMvc;

    private EventQueryService eventQueryService;

    @BeforeEach
    void setUp() {
        eventQueryService = mock(EventQueryService.class);
        mockMvc = MockMvcBuilders.standaloneSetup(new CustomerEventController(eventQueryService))
                .setControllerAdvice(new GlobalExceptionHandler())
                .build();
    }

    private static Event sampleEvent(String eventId) {
        Instant now = Instant.parse("2025-01-01T00:00:00Z");
        return new Event(eventId, "cust-1", EventType.PAYMENT, new BigDecimal("10.00"), "USD",
                now, EventStatus.RECEIVED, now, now, null, 0, null, Map.of());
    }

    @Test
    void getCustomerEvents_fromAfterToReturns400() throws Exception {
        when(eventQueryService.getCustomerEvents(anyString(), any(), any(), any(), anyInt(), anyInt()))
                .thenThrow(ApiException.badRequest("'from' must not be after 'to'"));

        mockMvc.perform(get("/api/customers/cust-1/events")
                        .param("from", "2025-01-02T00:00:00Z")
                        .param("to", "2025-01-01T00:00:00Z"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.type").value(ProblemTypes.VALIDATION_FAILED.toString()));
    }

    @Test
    void getCustomerEvents_nonNumericPageReturns400WithValidationFailedType() throws Exception {
        // Exercises MethodArgumentTypeMismatchException (Spring's own query-param binding failure),
        // a different code path from ApiException.badRequest but still mapped to validation-failed.
        mockMvc.perform(get("/api/customers/cust-1/events").param("page", "not-a-number"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.type").value(ProblemTypes.VALIDATION_FAILED.toString()));
    }
}
