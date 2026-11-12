package dev.eventpipeline.service.api.validation;

import jakarta.validation.ConstraintValidator;
import jakarta.validation.ConstraintValidatorContext;

import java.util.Map;

/**
 * Validates the size of the metadata map and the length of its keys/values. Null and empty maps
 * are allowed; the "optional" requirement is enforced separately by the absence of {@code @NotNull}.
 */
public class MetadataValidator implements ConstraintValidator<ValidMetadata, Map<String, String>> {

    public static final int MAX_ENTRIES = 20;
    public static final int MAX_KEY_LENGTH = 50;
    public static final int MAX_VALUE_LENGTH = 200;

    @Override
    public boolean isValid(Map<String, String> value, ConstraintValidatorContext context) {
        if (value == null || value.isEmpty()) {
            return true;
        }
        if (value.size() > MAX_ENTRIES) {
            return false;
        }
        for (Map.Entry<String, String> entry : value.entrySet()) {
            String key = entry.getKey();
            String val = entry.getValue();
            if (key == null || key.isEmpty() || key.length() > MAX_KEY_LENGTH) {
                return false;
            }
            if (val != null && val.length() > MAX_VALUE_LENGTH) {
                return false;
            }
        }
        return true;
    }
}
