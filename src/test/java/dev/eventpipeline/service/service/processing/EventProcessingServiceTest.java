package dev.eventpipeline.service.service.processing;

import dev.eventpipeline.service.config.ProcessingProperties;
import dev.eventpipeline.service.domain.Event;
import dev.eventpipeline.service.domain.EventStatus;
import dev.eventpipeline.service.repository.EventRepository;
import org.junit.jupiter.api.Test;
import org.springframework.dao.DataAccessResourceFailureException;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Optional;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class EventProcessingServiceTest {

    private final Instant now = Instant.parse("2026-11-11T12:00:00Z");
    private final EventRepository repository = mock(EventRepository.class);

    @Test
    void capturesClaimTokenBeforeMutableBusinessStepAndDoesNotAcknowledgeLostCompletion() {
        Event claim = claimedEvent(1);
        when(repository.claimForProcessing("event-1", now)).thenReturn(Optional.of(claim));
        when(repository.markProcessed("event-1", 1, now)).thenReturn(Optional.empty());
        EventProcessingService service = service(event -> event.setAttempts(99));

        assertThat(service.process("event-1", "message-1")).isEqualTo(ProcessingOutcome.IN_PROGRESS_ELSEWHERE);

        verify(repository).markProcessed("event-1", 1, now);
        verify(repository, never()).markProcessed("event-1", 99, now);
    }

    @Test
    void lostRetryResetDoesNotOverwriteOrAcknowledgeTheNewOwner() {
        when(repository.claimForProcessing("event-1", now)).thenReturn(Optional.of(claimedEvent(1)));
        when(repository.retryProcessing(eq("event-1"), eq(1), any(String.class), eq(now)))
                .thenReturn(Optional.empty());
        EventProcessingService service = service(event -> { throw new IllegalStateException("business failure"); });

        assertThat(service.process("event-1", "message-1")).isEqualTo(ProcessingOutcome.IN_PROGRESS_ELSEWHERE);

        verify(repository, never()).markFailed(any(), eq(1), any(), any());
        verify(repository, never()).markProcessed(any(), eq(1), any());
    }

    /** Builds the service around a processor double whose business step runs {@code businessStep}. */
    private EventProcessingService service(Consumer<Event> businessStep) {
        EventProcessor processor = mock(EventProcessor.class);
        doAnswer(invocation -> {
            businessStep.accept(invocation.getArgument(0));
            return null;
        }).when(processor).process(any(Event.class));
        return new EventProcessingService(repository, processor, Clock.fixed(now, ZoneOffset.UTC),
                new ProcessingProperties(3, 5, 60));
    }

    private Event claimedEvent(int attempts) {
        Event event = new Event();
        event.setEventId("event-1");
        event.setStatus(EventStatus.PROCESSING);
        event.setAttempts(attempts);
        return event;
    }
}
