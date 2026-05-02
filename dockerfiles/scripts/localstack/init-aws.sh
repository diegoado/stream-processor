#!/bin/bash
set -euo pipefail

REGION="us-east-1"
ENDPOINT="http://localhost:4566"
AWSCLI="awslocal"

echo "==> Creating S3 bucket..."
$AWSCLI s3 mb s3://stream-processor --region "$REGION"

echo "==> Uploading schema to S3..."
$AWSCLI s3 cp /tmp/schemas/event_schema.json s3://stream-processor/schemas/event_schema.json --region "$REGION"

echo "==> Creating SNS topic..."
TOPIC_ARN=$($AWSCLI sns create-topic --name events-topic --region "$REGION" --query 'TopicArn' --output text)
echo "    Topic ARN: $TOPIC_ARN"

echo "==> Creating SQS queues..."
for TENANT in tenant-a tenant-b tenant-c; do
  QUEUE_URL=$($AWSCLI sqs create-queue --queue-name "${TENANT}-queue" --region "$REGION" --query 'QueueUrl' --output text)
  QUEUE_ARN=$($AWSCLI sqs get-queue-attributes --queue-url "$QUEUE_URL" --attribute-names QueueArn --region "$REGION" --query 'Attributes.QueueArn' --output text)

  echo "    ${TENANT}-queue: $QUEUE_URL"

  echo "==> Subscribing ${TENANT}-queue to SNS with filter policy..."
  $AWSCLI sns subscribe \
    --topic-arn "$TOPIC_ARN" \
    --protocol sqs \
    --notification-endpoint "$QUEUE_ARN" \
    --attributes "{\"FilterPolicy\":\"{\\\"tenant_id\\\":[\\\"${TENANT}\\\"]}\"}" \
    --region "$REGION"
done

echo "==> LocalStack init complete."
