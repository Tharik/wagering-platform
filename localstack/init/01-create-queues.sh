#!/bin/bash

set -euo pipefail

echo "Creating SQS queues..."

create_fifo_queue_with_dlq() {
  local QUEUE_NAME="$1"
  local DLQ_NAME="$2"
  local VISIBILITY_TIMEOUT="$3"
  local RECEIVE_WAIT_TIME="$4"

  echo "Creating DLQ: $DLQ_NAME"

  DLQ_URL=$(awslocal sqs get-queue-url \
    --queue-name "$DLQ_NAME" \
    --query 'QueueUrl' \
    --output text 2>/dev/null || \
    awslocal sqs create-queue \
      --queue-name "$DLQ_NAME" \
      --attributes '{"FifoQueue":"true"}' \
      --query 'QueueUrl' \
      --output text)
  DLQ_URL="http://localhost:4566/${DLQ_URL#*4566/}"

  DLQ_ARN=$(awslocal sqs get-queue-attributes \
    --queue-url "$DLQ_URL" \
    --attribute-names QueueArn \
    --query 'Attributes.QueueArn' \
    --output text)

  REDRIVE_POLICY=$(printf \
    '{"deadLetterTargetArn":"%s","maxReceiveCount":"3"}' \
    "$DLQ_ARN")

  ATTRIBUTES=$(printf \
    '{"RedrivePolicy":"%s","VisibilityTimeout":"%s","ReceiveMessageWaitTimeSeconds":"%s"}' \
    "$(echo "$REDRIVE_POLICY" | sed 's/"/\\"/g')" \
    "$VISIBILITY_TIMEOUT" \
    "$RECEIVE_WAIT_TIME")

  echo "Creating queue: $QUEUE_NAME"

  QUEUE_URL=$(awslocal sqs get-queue-url \
    --queue-name "$QUEUE_NAME" \
    --query 'QueueUrl' \
    --output text 2>/dev/null || \
    awslocal sqs create-queue \
      --queue-name "$QUEUE_NAME" \
      --attributes '{"FifoQueue":"true"}' \
      --query 'QueueUrl' \
      --output text)
  QUEUE_URL="http://localhost:4566/${QUEUE_URL#*4566/}"

  awslocal sqs set-queue-attributes \
    --queue-url "$QUEUE_URL" \
    --attributes "$ATTRIBUTES"
}

# Outbound domain events published by the transactional outbox.
create_fifo_queue_with_dlq \
  "wager-events.fifo" \
  "wager-events-dlq.fifo" \
  "30" \
  "0"

# Inbound wager transactions consumed by the application.
create_fifo_queue_with_dlq \
  "wager-transactions.fifo" \
  "wager-transactions-dlq.fifo" \
  "60" \
  "10"

echo "SQS queues created."
