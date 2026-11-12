package dev.eventpipeline.service.config;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.IndexOptions;
import lombok.extern.slf4j.Slf4j;
import org.bson.Document;
import org.springframework.boot.ApplicationRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.mongodb.core.MongoTemplate;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

@Configuration
@Slf4j
public class EventCollectionInitializer {

    private static final String COLLECTION_NAME = "events";

    @Bean
    public ApplicationRunner initializeEventCollection(MongoTemplate mongoTemplate) {
        return args -> {
            MongoDatabase database = mongoTemplate.getDb();
            ArrayList<String> collectionNames = database.listCollectionNames().into(new ArrayList<>());

            if (!collectionNames.contains(COLLECTION_NAME)) {
                Document validator = new Document("$jsonSchema", new Document("bsonType", "object")
                        .append("required", List.of(
                                "customerId", "type", "amount", "currency", "timestamp",
                                "status", "createdAt", "updatedAt", "attempts"))
                        .append("properties", new Document()
                                .append("_id", new Document("bsonType", "string"))
                                .append("customerId", new Document("bsonType", "string"))
                                .append("type", new Document("bsonType", "string").append("minLength", 1))
                                .append("amount", new Document("bsonType", "decimal"))
                                .append("currency", new Document("bsonType", "string").append("pattern", "^[A-Z]{3}$"))
                                .append("timestamp", new Document("bsonType", "date"))
                                .append("status", new Document("enum", List.of("RECEIVED", "PROCESSING", "PROCESSED", "FAILED")))
                                .append("createdAt", new Document("bsonType", "date"))
                                .append("updatedAt", new Document("bsonType", "date"))
                                .append("processedAt", new Document("bsonType", "date"))
                                .append("attempts", new Document("bsonType", "int"))
                                .append("failureReason", new Document("bsonType", "string"))
                                .append("metadata", new Document("bsonType", "object"))
                        )
                        .append("additionalProperties", true));

                database.runCommand(new Document("create", COLLECTION_NAME)
                        .append("validator", validator)
                        .append("validationLevel", "strict")
                        .append("validationAction", "error"));

                log.info("Created '{}' collection with strict validator.", COLLECTION_NAME);
            } else {
                log.info("Existing '{}' collection detected; leaving validator unchanged. Schema evolution requires a migration run with an elevated role.", COLLECTION_NAME);
            }

            ensureIndexes(database.getCollection(COLLECTION_NAME));
        };
    }

    // Only creates indexes that are missing, so startup issues no index command (and needs no
    // createIndex privilege) when everything already exists.
    private void ensureIndexes(MongoCollection<Document> events) {
        Set<String> existing = new HashSet<>();
        events.listIndexes().forEach(index -> existing.add(index.getString("name")));

        createIfMissing(events, existing, "customerId_1_createdAt_-1",
                new Document("customerId", 1).append("createdAt", -1));
        createIfMissing(events, existing, "status_1_updatedAt_1",
                new Document("status", 1).append("updatedAt", 1));
    }

    private void createIfMissing(MongoCollection<Document> events, Set<String> existing, String name, Document keys) {
        if (existing.contains(name)) {
            return;
        }
        events.createIndex(keys, new IndexOptions().name(name));
        log.info("Created missing index '{}' on '{}'.", name, COLLECTION_NAME);
    }
}
