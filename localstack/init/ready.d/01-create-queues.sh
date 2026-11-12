#!/bin/bash
set -euo pipefail

awslocal sqs create-queue --queue-name "$AWS_SQS_DLQ_NAME" >/dev/null
DLQ_URL=$(awslocal sqs get-queue-url --queue-name "$AWS_SQS_DLQ_NAME" --query 'QueueUrl' --output text)
DLQ_ARN=$(awslocal sqs get-queue-attributes --queue-url "$DLQ_URL" --attribute-names QueueArn --query 'Attributes.QueueArn' --output text)

awslocal sqs create-queue \
  --queue-name "$AWS_SQS_QUEUE_NAME" \
  --attributes "{\"RedrivePolicy\":\"{\\\"deadLetterTargetArn\\\":\\\"$DLQ_ARN\\\",\\\"maxReceiveCount\\\":\\\"${PROCESSING_MAX_ATTEMPTS:-3}\\\"}\"}" \
  >/dev/null
