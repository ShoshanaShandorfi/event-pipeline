package dev.eventpipeline.service;

import com.mongodb.MongoWriteException;
import com.mongodb.client.MongoCollection;
import dev.eventpipeline.service.config.EventCollectionInitializer;
import dev.eventpipeline.service.domain.Event;
import dev.eventpipeline.service.domain.EventType;
import dev.eventpipeline.service.domain.EventStatus;
import dev.eventpipeline.service.repository.EventRepository;
import org.bson.Document;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.data.mongo.DataMongoTest;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.mongodb.core.MongoTemplate;
import dev.eventpipeline.service.config.MongoTestcontainersConfig;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@DataMongoTest
class MongoEventRepositoryIntegrationTest extends MongoTestcontainersConfig {

    @Autowired
    private MongoTemplate mongoTemplate;

    @Autowired
    private EventRepository eventRepository;

    @BeforeEach
    void setUp() throws Exception {
        mongoTemplate.getDb().drop();
        new EventCollectionInitializer().initializeEventCollection(mongoTemplate)
                .run(new org.springframework.boot.DefaultApplicationArguments());
    }

    @Test
    void initializerIsIdempotentAndCreatesValidatorAndIndexes() {
        // setUp() already created the "events" collection (with validator and indexes) via the initializer.
        // Re-running the initializer should be a no-op and must not fail even though everything already exists.
        EventCollectionInitializer initializer = new EventCollectionInitializer();
        try {
            initializer.initializeEventCollection(mongoTemplate).run(new org.springframework.boot.DefaultApplicationArguments());
            initializer.initializeEventCollection(mongoTemplate).run(new org.springframework.boot.DefaultApplicationArguments());
        } catch (Exception ex) {
            throw new AssertionError("Initializer should be idempotent", ex);
        }

        assertThat(mongoTemplate.getCollection("events").listIndexes()).hasSizeGreaterThanOrEqualTo(2);
        assertThat(mongoTemplate.getCollection("events").listIndexes()).anySatisfy(index -> assertThat(index.get("name")).isIn("customerId_1_createdAt_-1", "status_1_updatedAt_1"));

        Object cursor = mongoTemplate.getDb().runCommand(new Document("listCollections", 1).append("filter", new Document("name", "events"))).get("cursor");
        assertThat(cursor).isNotNull();
    }

    @Test
    void validatorRejectsInvalidStatusAndCurrencyAndAcceptsMissingOptionalFields() {
        MongoCollection<Document> collection = mongoTemplate.getCollection("events");
        Document invalidStatus = new Document("_id", "bad-status")
                .append("customerId", "cust-1")
                .append("type", "PAYMENT")
                .append("amount", new org.bson.types.Decimal128(new BigDecimal("10.00")))
                .append("currency", "USD")
                .append("timestamp", java.util.Date.from(Instant.now().truncatedTo(ChronoUnit.MILLIS)))
                .append("status", "BOGUS")
                .append("createdAt", java.util.Date.from(Instant.now().truncatedTo(ChronoUnit.MILLIS)))
                .append("updatedAt", java.util.Date.from(Instant.now().truncatedTo(ChronoUnit.MILLIS)))
                .append("attempts", 0);

        assertThatThrownBy(() -> collection.insertOne(invalidStatus)).isInstanceOf(MongoWriteException.class);

        Document invalidCurrency = new Document("_id", "bad-currency")
                .append("customerId", "cust-1")
                .append("type", "PAYMENT")
                .append("amount", new org.bson.types.Decimal128(new BigDecimal("10.00")))
                .append("currency", "usd")
                .append("timestamp", java.util.Date.from(Instant.now().truncatedTo(ChronoUnit.MILLIS)))
                .append("status", "RECEIVED")
                .append("createdAt", java.util.Date.from(Instant.now().truncatedTo(ChronoUnit.MILLIS)))
                .append("updatedAt", java.util.Date.from(Instant.now().truncatedTo(ChronoUnit.MILLIS)))
                .append("attempts", 0);

        assertThatThrownBy(() -> collection.insertOne(invalidCurrency)).isInstanceOf(MongoWriteException.class);

        Document validWithoutOptionalFields = new Document("_id", "valid-no-optional")
                .append("customerId", "cust-1")
                .append("type", "PAYMENT")
                .append("amount", new org.bson.types.Decimal128(new BigDecimal("5.55")))
                .append("currency", "USD")
                .append("timestamp", java.util.Date.from(Instant.now().truncatedTo(ChronoUnit.MILLIS)))
                .append("status", "RECEIVED")
                .append("createdAt", java.util.Date.from(Instant.now().truncatedTo(ChronoUnit.MILLIS)))
                .append("updatedAt", java.util.Date.from(Instant.now().truncatedTo(ChronoUnit.MILLIS)))
                .append("attempts", 0);

        collection.insertOne(validWithoutOptionalFields);
        assertThat(collection.find(new Document("_id", "valid-no-optional")).first()).isNotNull();
    }

    @Test
    void duplicateKeyThrowsException() {
        Event event = new Event("same-id", "cust-1", EventType.PAYMENT, new BigDecimal("10.00"), "USD",
                Instant.now().truncatedTo(ChronoUnit.MILLIS), EventStatus.RECEIVED, Instant.now().truncatedTo(ChronoUnit.MILLIS), Instant.now().truncatedTo(ChronoUnit.MILLIS), null, 0, null, Map.of());
        eventRepository.save(event);

        Event duplicate = new Event("same-id", "cust-1", EventType.PAYMENT, new BigDecimal("11.00"), "USD",
                Instant.now().truncatedTo(ChronoUnit.MILLIS), EventStatus.RECEIVED, Instant.now().truncatedTo(ChronoUnit.MILLIS), Instant.now().truncatedTo(ChronoUnit.MILLIS), null, 0, null, Map.of());

        // save() performs an upsert by id and would silently overwrite; insert() enforces a true duplicate-key check.
        assertThatThrownBy(() -> eventRepository.insert(duplicate)).isInstanceOf(RuntimeException.class);
    }

}