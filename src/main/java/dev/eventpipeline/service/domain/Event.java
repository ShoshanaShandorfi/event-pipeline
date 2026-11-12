package dev.eventpipeline.service.domain;

import lombok.Builder;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.data.mongodb.core.mapping.Field;
import org.springframework.data.mongodb.core.mapping.FieldType;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Map;

@Document(collection = "events")
public class Event {

    /**
     * timestamp is the client-reported event time; createdAt is the service receive time.
     * The service sets createdAt on receipt, so it is the trusted server-side time used for indexing and queries.
     */
    @Id
    private String eventId;

    private String customerId;
    private EventType type;
    @Field(targetType = FieldType.DECIMAL128)
    private BigDecimal amount;
    private String currency;
    private Instant timestamp;
    private EventStatus status;
    private Instant createdAt;
    private Instant updatedAt;
    private Instant processedAt;
    private int attempts;
    private String failureReason;
    private Map<String, String> metadata;

    public Event() {
    }

    @Builder
    public Event(String eventId, String customerId, EventType type, BigDecimal amount, String currency,
                 Instant timestamp, EventStatus status, Instant createdAt, Instant updatedAt,
                 Instant processedAt, int attempts, String failureReason, Map<String, String> metadata) {
        this.eventId = eventId;
        this.customerId = customerId;
        this.type = type;
        this.amount = amount;
        this.currency = currency;
        this.timestamp = timestamp;
        this.status = status;
        this.createdAt = createdAt;
        this.updatedAt = updatedAt;
        this.processedAt = processedAt;
        this.attempts = attempts;
        this.failureReason = failureReason;
        this.metadata = metadata;
    }

    public String getEventId() {
        return eventId;
    }

    public void setEventId(String eventId) {
        this.eventId = eventId;
    }

    public String getCustomerId() {
        return customerId;
    }

    public void setCustomerId(String customerId) {
        this.customerId = customerId;
    }

    public EventType getType() {
        return type;
    }

    public void setType(EventType type) {
        this.type = type;
    }

    public BigDecimal getAmount() {
        return amount;
    }

    public void setAmount(BigDecimal amount) {
        this.amount = amount;
    }

    public String getCurrency() {
        return currency;
    }

    public void setCurrency(String currency) {
        this.currency = currency;
    }

    public Instant getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Instant timestamp) {
        this.timestamp = timestamp;
    }

    public EventStatus getStatus() {
        return status;
    }

    public void setStatus(EventStatus status) {
        this.status = status;
    }

    public Instant getCreatedAt() {
        return createdAt;
    }

    public void setCreatedAt(Instant createdAt) {
        this.createdAt = createdAt;
    }

    public Instant getUpdatedAt() {
        return updatedAt;
    }

    public void setUpdatedAt(Instant updatedAt) {
        this.updatedAt = updatedAt;
    }

    public Instant getProcessedAt() {
        return processedAt;
    }

    public void setProcessedAt(Instant processedAt) {
        this.processedAt = processedAt;
    }

    public int getAttempts() {
        return attempts;
    }

    public void setAttempts(int attempts) {
        this.attempts = attempts;
    }

    public String getFailureReason() {
        return failureReason;
    }

    public void setFailureReason(String failureReason) {
        this.failureReason = failureReason;
    }

    public Map<String, String> getMetadata() {
        return metadata;
    }

    public void setMetadata(Map<String, String> metadata) {
        this.metadata = metadata;
    }
}
