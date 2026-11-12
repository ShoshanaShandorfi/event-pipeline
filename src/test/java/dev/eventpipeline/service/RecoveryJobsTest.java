package dev.eventpipeline.service;

import dev.eventpipeline.service.config.ProcessingProperties;
import dev.eventpipeline.service.config.RecoveryProperties;
import dev.eventpipeline.service.config.RelayProperties;
import dev.eventpipeline.service.domain.Event;
import dev.eventpipeline.service.domain.EventStatus;
import dev.eventpipeline.service.repository.EventRepository;
import dev.eventpipeline.service.messaging.ingestion.SqsEventPublisher;
import dev.eventpipeline.service.service.ingestion.ReceivedEventRelay;
import dev.eventpipeline.service.service.processing.StuckProcessingRecovery;
import org.junit.jupiter.api.Test;
import org.springframework.dao.DataAccessResourceFailureException;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Optional;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class RecoveryJobsTest {

    private static final Instant NOW = Instant.parse("2026-01-01T12:00:00Z");
    private static final Clock CLOCK = Clock.fixed(NOW, ZoneOffset.UTC);

    @Test
    void relaySurvivesMongoFailureAndPublishesAtNextTick() {
        EventRepository repository = mock(EventRepository.class);
        SqsEventPublisher publisher = mock(SqsEventPublisher.class);
        Event event = new Event();
        event.setEventId("after-outage");
        when(repository.leaseStaleReceived(any(), any()))
                .thenThrow(new DataAccessResourceFailureException("unavailable"))
                .thenReturn(Optional.of(event), Optional.empty());
        ReceivedEventRelay relay = new ReceivedEventRelay(repository, publisher, CLOCK,
                new RelayProperties(false, 1, 60, 100));

        relay.relayStaleEvents();
        relay.relayStaleEvents();

        verify(repository, times(3)).leaseStaleReceived(NOW.minusSeconds(60), NOW);
        verify(publisher).publish("after-outage");
    }

    @Test
    void recoverySurvivesMongoFailureAndContinuesAtNextTick() {
        EventRepository repository = mock(EventRepository.class);
        ProcessingProperties processing = mock(ProcessingProperties.class);
        when(processing.maxAttempts()).thenReturn(3);
        Event event = new Event();
        event.setStatus(EventStatus.RECEIVED);
        when(repository.recoverStaleProcessing(any(), any(), eq(3)))
                .thenThrow(new DataAccessResourceFailureException("unavailable"))
                .thenReturn(Optional.of(event), Optional.empty());
        StuckProcessingRecovery recovery = new StuckProcessingRecovery(repository, CLOCK,
                new RecoveryProperties(false, 1, 300), processing);

        recovery.recoverStaleEvents();
        recovery.recoverStaleEvents();

        verify(repository, times(3)).recoverStaleProcessing(NOW.minusSeconds(300), NOW, 3);
    }
}
