package dev.eventpipeline.service.architecture;

import com.tngtech.archunit.core.domain.JavaClasses;
import com.tngtech.archunit.core.importer.ClassFileImporter;
import com.tngtech.archunit.core.importer.ImportOption;
import com.tngtech.archunit.lang.ArchRule;
import org.junit.jupiter.api.Test;

import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.noClasses;

/**
 * Enforces the architecture constraint: the ingestion side (the REST API, the ingestion service,
 * SQS publisher, and RECEIVED relay) and the processing side (the SQS consumer, processing service,
 * and stuck-PROCESSING recovery) must never depend on each other directly. They communicate only
 * through SQS and MongoDB. Package predicates include every new relay/recovery job automatically.
 * The only packages allowed to be shared by both sides are {@code domain}, {@code repository}, and
 * {@code config} (property holders / client factories -- not business logic).
 */
class IngestionProcessingSeparationArchTest {

    private static final String BASE_PACKAGE = "dev.eventpipeline.service";

    private static final String[] INGESTION_PACKAGES = {
            "..api..",
            "..service.ingestion..",
            "..messaging.ingestion.."
    };

    private static final String[] PROCESSING_PACKAGES = {
            "..service.processing..",
            "..messaging.processing.."
    };

    private static final JavaClasses CLASSES = new ClassFileImporter()
            .withImportOption(ImportOption.Predefined.DO_NOT_INCLUDE_TESTS)
            .importPackages(BASE_PACKAGE);

    @Test
    void ingestionSideMustNotDependOnProcessingSide() {
        ArchRule rule = noClasses()
                .that().resideInAnyPackage(INGESTION_PACKAGES)
                .should().dependOnClassesThat().resideInAnyPackage(PROCESSING_PACKAGES)
                .because("the ingestion side (api, ingestion service, SQS publisher, RECEIVED relay) must only "
                        + "communicate with the processing side through SQS and MongoDB, never directly");

        rule.check(CLASSES);
    }

    @Test
    void processingSideMustNotDependOnIngestionSide() {
        ArchRule rule = noClasses()
                .that().resideInAnyPackage(PROCESSING_PACKAGES)
                .should().dependOnClassesThat().resideInAnyPackage(INGESTION_PACKAGES)
                .because("the processing side (SQS consumer, processing service, stuck-PROCESSING recovery) must only "
                        + "communicate with the ingestion side through SQS and MongoDB, never directly");

        rule.check(CLASSES);
    }
}
