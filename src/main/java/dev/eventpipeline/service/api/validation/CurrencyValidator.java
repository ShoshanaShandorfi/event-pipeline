package dev.eventpipeline.service.api.validation;

import jakarta.validation.ConstraintValidator;
import jakarta.validation.ConstraintValidatorContext;

import java.util.Currency;

/**
 * Accepts only codes that {@link Currency#getInstance(String)} recognises. That lookup is already
 * case-sensitive and rejects anything that is not a known three-letter ISO 4217 code, so no extra
 * format checks are needed.
 */
public class CurrencyValidator implements ConstraintValidator<ValidCurrency, String> {

    @Override
    public boolean isValid(String value, ConstraintValidatorContext context) {
        if (value == null) {
            return true;
        }
        try {
            Currency.getInstance(value);
            return true;
        } catch (IllegalArgumentException e) {
            return false;
        }
    }
}
