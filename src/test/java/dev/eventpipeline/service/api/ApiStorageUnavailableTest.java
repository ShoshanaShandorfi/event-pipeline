package dev.eventpipeline.service.api;

import dev.eventpipeline.service.api.exception.ProblemTypes;
import dev.eventpipeline.service.domain.Event;
import dev.eventpipeline.service.repository.EventRepository;
import dev.eventpipeline.service.service.ingestion.EventIngestionService;
import dev.eventpipeline.service.messaging.ingestion.SqsEventPublisher;
import dev.eventpipeline.service.service.query.EventQueryService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

/** Real ingestion/controller/advice, exercised through standalone MVC without a Boot context. */
class ApiStorageUnavailableTest {
    private MockMvc mvc;
    private EventRepository repository;
    private SqsEventPublisher publisher;

    @BeforeEach
    void setUp() {
        repository = mock(EventRepository.class);
        publisher = mock(SqsEventPublisher.class);
        EventQueryService queryService = mock(EventQueryService.class);
        Clock clock = Clock.fixed(Instant.parse("2026-11-11T12:00:00Z"), ZoneOffset.UTC);
        EventIngestionService ingestionService = new EventIngestionService(repository, clock, publisher);
        mvc = MockMvcBuilders.standaloneSetup(new EventController(ingestionService, queryService))
                .setControllerAdvice(new GlobalExceptionHandler())
                .build();
    }

    @Test
    void unavailableMongoReturnsRetryAfterAndCleanProblemWithoutPublishing() throws Exception {
        String privateDetail = "mongodb://private-host unavailable internal-driver-detail";
        when(repository.insert(any(Event.class))).thenThrow(new DataAccessResourceFailureException(privateDetail));

        var response = mvc.perform(post("/api/events").contentType(MediaType.APPLICATION_JSON).content("""
                {"eventId":"storage-outage","customerId":"cust-1","type":"PAYMENT",
                 "amount":10.00,"currency":"USD","timestamp":"2025-01-01T00:00:00Z"}
                """))
                .andExpect(status().isServiceUnavailable())
                .andExpect(header().string("Retry-After", "5"))
                .andExpect(content().contentTypeCompatibleWith(MediaType.APPLICATION_PROBLEM_JSON))
                .andExpect(jsonPath("$.type").value(ProblemTypes.SERVICE_UNAVAILABLE.toString()))
                .andExpect(jsonPath("$.title").value("Service Unavailable"))
                .andExpect(jsonPath("$.status").value(503))
                .andExpect(jsonPath("$.detail").value("Event storage is temporarily unavailable; retry later"))
                .andExpect(jsonPath("$.instance").value("/api/events"))
                .andReturn().getResponse();

        assertThat(response.getContentAsString()).doesNotContain(privateDetail, "stackTrace", "exception", "private-host");
        verifyNoInteractions(publisher);
    }
}
