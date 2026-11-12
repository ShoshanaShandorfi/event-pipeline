package dev.eventpipeline.service.config;

import org.springframework.stereotype.Component;

/**
 * Fails startup when settings from different groups contradict each other. Each properties record
 * already validates its own values; this covers the rules that span records.
 *
 * <p>Stale-PROCESSING recovery must wait longer than the SQS visibility timeout. Otherwise it would
 * reset an event while its message is still invisible to other consumers and its worker may still be
 * running. (The maximum expected processing time cannot be checked here; it is a deployment concern.)
 */
@Component
public class PipelineSettingsValidator {

    public PipelineSettingsValidator(SqsConsumerProperties consumer, RecoveryProperties recovery) {
        if (consumer.enabled() && recovery.enabled()
                && recovery.staleProcessingThresholdSeconds() <= consumer.visibilityTimeoutSeconds()) {
            throw new IllegalStateException("pipeline.recovery.stale-processing-threshold-seconds ("
                    + recovery.staleProcessingThresholdSeconds() + ") must be greater than "
                    + "aws.sqs.consumer.visibility-timeout-seconds (" + consumer.visibilityTimeoutSeconds() + ")");
        }
    }
}
