#!/bin/bash

set -euo pipefail

echo "Creating SQS queues..."

create_fifo_queue_with_dlq() {
  local QUEUE_NAME="$1"
  local DLQ_NAME="$2"

  echo "Creating DLQ: $DLQ_NAME"

  DLQ_URL=$(awslocal sqs create-queue \
    --queue-name "$DLQ_NAME" \
    --attributes '{"FifoQueue":"true"}' \
    --query 'QueueUrl' \
    --output text)

  DLQ_ARN=$(awslocal sqs get-queue-attributes \
    --queue-url "$DLQ_URL" \
    --attribute-names QueueArn \
    --query 'Attributes.QueueArn' \
    --output text)

  REDRIVE_POLICY=$(printf \
    '{"deadLetterTargetArn":"%s","maxReceiveCount":"3"}' \
    "$DLQ_ARN")

  ATTRIBUTES=$(printf \
    '{"FifoQueue":"true","RedrivePolicy":"%s"}' \
    "$(echo "$REDRIVE_POLICY" | sed 's/"/\\"/g')")

  echo "Creating queue: $QUEUE_NAME"

  awslocal sqs create-queue \
    --queue-name "$QUEUE_NAME" \
    --attributes "$ATTRIBUTES"
}

# Outbound domain events published by the transactional outbox.
create_fifo_queue_with_dlq \
  "wager-events.fifo" \
  "wager-events-dlq.fifo"

# Inbound wager transactions consumed by the application.
create_fifo_queue_with_dlq \
  "wager-transactions.fifo" \
  "wager-transactions-dlq.fifo"

echo "SQS queues created."