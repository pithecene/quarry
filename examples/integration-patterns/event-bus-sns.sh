#!/bin/bash
# Event-bus pattern: Publish run completion to AWS SNS
#
# Usage: ./event-bus-sns.sh <run-id> <source> <outcome> <storage-path>
#
# This script demonstrates publishing a run completion event to SNS
# after a Quarry run exits. Downstream consumers (Lambda, ECS, etc.)
# subscribe to the SNS topic via SQS.
#
# Prerequisites:
# - AWS CLI configured with appropriate credentials
# - SNS topic ARN (set via environment variable or modify script)

set -euo pipefail

RUN_ID="${1:?Usage: $0 <run-id> <source> <outcome> <storage-path>}"
SOURCE="${2:?}"
OUTCOME="${3:?}"
STORAGE_PATH="${4:?}"

# SNS topic ARN - set via environment or modify here
SNS_TOPIC_ARN="${QUARRY_SNS_TOPIC:-arn:aws:sns:us-east-1:123456789012:quarry-runs}"

# Build event payload
TIMESTAMP=$(date -u +%Y-%m-%dT%H:%M:%SZ)
DAY=$(date -u +%Y-%m-%d)

EVENT_PAYLOAD=$(cat <<EOF
{
  "event_type": "run_completed",
  "run_id": "${RUN_ID}",
  "source": "${SOURCE}",
  "day": "${DAY}",
  "outcome": "${OUTCOME}",
  "storage_path": "${STORAGE_PATH}/source=${SOURCE}/day=${DAY}/run_id=${RUN_ID}",
  "timestamp": "${TIMESTAMP}"
}
EOF
)

echo "Publishing run completion event to SNS..."
echo "Topic: ${SNS_TOPIC_ARN}"
echo "Payload: ${EVENT_PAYLOAD}"

# Publish to SNS
# In production, add retry logic with exponential backoff
if command -v aws &> /dev/null; then
  aws sns publish \
    --topic-arn "${SNS_TOPIC_ARN}" \
    --message "${EVENT_PAYLOAD}" \
    --message-attributes '{
      "run_id": {"DataType": "String", "StringValue": "'"${RUN_ID}"'"},
      "source": {"DataType": "String", "StringValue": "'"${SOURCE}"'"},
      "outcome": {"DataType": "String", "StringValue": "'"${OUTCOME}"'"}
    }'
  echo "Event published successfully"
else
  echo "AWS CLI not found - this is a conceptual example"
  echo "Would publish: ${EVENT_PAYLOAD}"
fi
