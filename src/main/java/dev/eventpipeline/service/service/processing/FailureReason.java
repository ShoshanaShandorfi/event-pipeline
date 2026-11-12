package dev.eventpipeline.service.service.processing;

import dev.eventpipeline.service.domain.Event;

import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.stream.Stream;

/** Produces bounded operational diagnostics, never a stack trace or serialized event. */
final class FailureReason {

    private static final int MAX_MESSAGE_LENGTH = 200;

    private FailureReason() {
    }

    static String from(Exception failure, Event event) {
        String message = normalize(Objects.toString(failure.getMessage(), ""));
        // A message carrying structured data or credential markers is unsafe to retain at all. Do
        // not try to parse arbitrary payloads or guess where a potentially secret value ends.
        if (isSensitive(message)) {
            message = "[redacted sensitive diagnostic]";
        } else {
            message = redactEventValues(message, event);
        }
        if (message.length() > MAX_MESSAGE_LENGTH) {
            message = message.substring(0, MAX_MESSAGE_LENGTH);
        }
        return failure.getClass().getName() + (message.isBlank() ? "" : ": " + message);
    }

    private static boolean isSensitive(String message) {
        return message.matches("(?is).*(payload|metadata|password|passwd|secret|token|credential|authorization|api.?key).*")
                || message.contains("{") || message.contains("[") || message.contains("://");
    }

    private static String redactEventValues(String message, Event event) {
        Stream<String> metadata = event.getMetadata() == null ? Stream.empty()
                : event.getMetadata().entrySet().stream().flatMap(e -> Stream.of(e.getKey(), e.getValue()));
        Stream<String> fields = Stream.of(event.getEventId(), event.getCustomerId(), Objects.toString(event.getType(), null),
                event.getCurrency(), Objects.toString(event.getAmount(), null), Objects.toString(event.getTimestamp(), null));
        // Longest first avoids partially hiding one field before matching a longer field value.
        List<String> values = Stream.concat(fields, metadata)
                .filter(Objects::nonNull)
                .map(FailureReason::normalize)
                .filter(v -> !v.isEmpty())
                .sorted(Comparator.comparingInt(String::length).reversed())
                .toList();
        for (String value : values) {
            message = message.replace(value, "[redacted]");
        }
        return message;
    }

    private static String normalize(String value) {
        return value.replaceAll("[\\p{Cntrl}\\p{Cf}]", " ");
    }
}
