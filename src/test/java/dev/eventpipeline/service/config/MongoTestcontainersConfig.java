package dev.eventpipeline.service.config;

import org.junit.jupiter.api.Tag;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.utility.DockerImageName;

/** The inherited tag keeps Mongo/LocalStack tests in the opt-in full-suite profile. */
@Tag("integration")
public abstract class MongoTestcontainersConfig {

    private static final MongoDBContainer MONGO = new MongoDBContainer(DockerImageName.parse("mongo:7.0"));

    @DynamicPropertySource
    static void mongoProperties(DynamicPropertyRegistry registry) {
        if (!MONGO.isRunning()) {
            MONGO.start();
        }

        registry.add("spring.data.mongodb.uri", () -> MONGO.getReplicaSetUrl("test"));
        registry.add("spring.data.mongodb.database", () -> "test");
    }
}
