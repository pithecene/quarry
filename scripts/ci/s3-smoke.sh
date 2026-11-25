#!/usr/bin/env bash
# Run the S3 smokes against a disposable RustFS container, which
# honors the conditional writes that Lode commits with.
# Usage: scripts/ci/s3-smoke.sh (after task build)

set -euo pipefail

# The version that Lode's own integration tests use (lode/s3/docker-compose.yaml).
image=rustfs/rustfs:1.0.3
export AWS_ACCESS_KEY_ID=quarrysmoke AWS_SECRET_ACCESS_KEY=quarrysmoke-local
unset AWS_SESSION_TOKEN AWS_PROFILE AWS_DEFAULT_PROFILE

container=$(docker create --memory 1g --publish 127.0.0.1::9000 \
  --env RUSTFS_ACCESS_KEY="$AWS_ACCESS_KEY_ID" \
  --env RUSTFS_SECRET_KEY="$AWS_SECRET_ACCESS_KEY" "$image")
trap 'docker rm --force "$container" > /dev/null' EXIT
docker start "$container" > /dev/null
endpoint="http://$(docker port "$container" 9000/tcp)"
curl --silent --fail --retry 60 --retry-delay 1 --retry-all-errors --max-time 5 \
  "$endpoint/health/ready" > /dev/null
curl --silent --show-error --fail --aws-sigv4 aws:amz:us-east-1:s3 \
  --user "$AWS_ACCESS_KEY_ID:$AWS_SECRET_ACCESS_KEY" --request PUT "$endpoint/smoke"
for smoke in lifecycle concurrent; do
  pnpm exec tsx "scripts/ci/s3/$smoke.ts" "${QUARRY_BINARY:-quarry/quarry}" "$endpoint"
done
