package dev.eventpipeline.service.api.validation;

import jakarta.validation.ConstraintValidator;
import jakarta.validation.ConstraintValidatorContext;

import java.util.Currency;

/**
 * Accepts only uppercase three-letter codes that {@link Currency#getInstance(String)} recognises.
 * The uppercase check is explicit because {@code Currency.getInstance} is case-sensitive anyway,
 * but a clear precondition keeps the intent obvious and matches the Mongo schema pattern.
 */
public class CurrencyValidator implements ConstraintValidator<ValidCurrency, String> {

    @Override
    public boolean isValid(String value, ConstraintValidatorContext context) {
        if (value == null) {
            return true;
        }
        try {
            Currency.getInstance(value);
            return value.length() == 3 && value.equals(value.toUpperCase(java.util.Locale.ROOT));
        } catch (IllegalArgumentException e) {
            return false;
        }
    }
}
