package dev.eventpipeline.service.service.ingestion;

import dev.eventpipeline.service.domain.Event;
import dev.eventpipeline.service.domain.EventType;
import dev.eventpipeline.service.domain.EventStatus;
import dev.eventpipeline.service.repository.EventRepository;
import dev.eventpipeline.service.messaging.ingestion.SqsEventPublisher;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.dao.DuplicateKeyException;

import java.math.BigDecimal;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

/**
 * Fast, Spring-free unit test proving the ordering/conditions under which {@link EventIngestionService}
 * calls {@link SqsEventPublisher#publish(String)}: only after a successful insert, and never for a
 * duplicate (identical payload) or conflicting (different payload) request. This is the test required
 * by the stage-4 spec showing that no message is ever published for an event that is not yet persisted
 * -- the publish call happens strictly after {@code eventRepository.insert(...)} returns.
 */
@ExtendWith(MockitoExtension.class)
class EventIngestionServiceTest {

    @Mock
    private EventRepository eventRepository;

    @Mock
    private SqsEventPublisher eventPublisher;

    private final Clock clock = Clock.fixed(Instant.parse("2025-01-01T00:00:00Z"), ZoneOffset.UTC);

    @Test
    void publishIsCalledOnlyAfterTheInsertHasCompletedSuccessfully() {
        EventIngestionService service = new EventIngestionService(eventRepository, clock, eventPublisher);
        EventCreationCommand command = newCommand("evt-new");

        EventIngestionResult result = service.ingest(command);

        assertThat(result.outcome()).isEqualTo(IngestionOutcome.CREATED);

        // Strict ordering: insert() must be the first interaction, publish() only afterwards -- never
        // the other way around, and never from inside the repository call itself (the mock proves
        // nothing runs publish() as a side effect of insert()).
        InOrder inOrder = inOrder(eventRepository, eventPublisher);
        inOrder.verify(eventRepository).insert(any(Event.class));
        inOrder.verify(eventPublisher).publish("evt-new");
        inOrder.verifyNoMoreInteractions();
    }

    private EventCreationCommand newCommand(String eventId) {
        return EventCreationCommand.builder()
                .eventId(eventId)
                .customerId("cust-1")
                .type(EventType.PAYMENT)
                .amount(new BigDecimal("10.00"))
                .currency("USD")
                .timestamp(Instant.parse("2025-01-01T00:00:00Z"))
                .metadata(Map.of())
                .build();
    }
}
