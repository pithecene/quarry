#!/bin/bash
# Event-bus pattern: forward a Quarry notification to AWS SNS
#
# Usage: ./event-bus-sns.sh < notification.json
#
# The input is the run_completed notification that Quarry's webhook or Redis
# adapter delivers. The script publishes it unchanged, so a consumer can read
# the run through its locator (see event-bus-handler.ts).
#
# Prerequisites:
# - AWS CLI configured with appropriate credentials
# - SNS topic ARN (set QUARRY_SNS_TOPIC)

set -euo pipefail

topic="${QUARRY_SNS_TOPIC:?set QUARRY_SNS_TOPIC to the topic ARN}"
notification=$(cat)

aws sns publish --topic-arn "$topic" --message "$notification"
