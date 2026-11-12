package dev.eventpipeline.service.api.validation;

import jakarta.validation.Constraint;
import jakarta.validation.Payload;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Bounds the metadata map on an incoming event so a client cannot submit an unbounded number of
 * entries or oversized keys/values into the request.
 */
@Target({ElementType.FIELD, ElementType.RECORD_COMPONENT, ElementType.PARAMETER, ElementType.METHOD})
@Retention(RetentionPolicy.RUNTIME)
@Constraint(validatedBy = MetadataValidator.class)
public @interface ValidMetadata {

    String message() default "metadata must have at most " + MetadataValidator.MAX_ENTRIES
            + " entries with keys up to " + MetadataValidator.MAX_KEY_LENGTH
            + " characters and values up to " + MetadataValidator.MAX_VALUE_LENGTH + " characters";

    Class<?>[] groups() default {};

    Class<? extends Payload>[] payload() default {};
}
