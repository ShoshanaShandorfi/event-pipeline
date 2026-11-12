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
import java.util.List;

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

                MongoCollection<Document> events = database.getCollection(COLLECTION_NAME);
                events.createIndex(new Document("customerId", 1).append("createdAt", -1),
                        new IndexOptions().name("customerId_1_createdAt_-1"));
                events.createIndex(new Document("status", 1).append("updatedAt", 1),
                        new IndexOptions().name("status_1_updatedAt_1"));
                log.info("Created '{}' collection with strict validator and required indexes.", COLLECTION_NAME);
                return;
            }

            log.info("Existing '{}' collection detected; leaving validator unchanged. Schema evolution requires a migration run with an elevated role.", COLLECTION_NAME);
        };
    }
}
