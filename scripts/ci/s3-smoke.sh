#!/usr/bin/env bash
# Run the S3 lifecycle smoke against a disposable RustFS container, which
# honors the conditional writes that Lode commits with.
# Usage: scripts/ci/s3-smoke.sh (after task build)

set -euo pipefail

# The version that Lode's own integration tests use (lode/s3/docker-compose.yaml).
image=rustfs/rustfs:1.0.3
export AWS_ACCESS_KEY_ID=quarrysmoke AWS_SECRET_ACCESS_KEY=quarrysmoke-local

name="quarry-s3-smoke-$$"
trap 'docker rm --force "$name" > /dev/null' EXIT
docker run --detach --name "$name" --memory 1g --publish 127.0.0.1::9000 \
  --env RUSTFS_ACCESS_KEY="$AWS_ACCESS_KEY_ID" \
  --env RUSTFS_SECRET_KEY="$AWS_SECRET_ACCESS_KEY" "$image" > /dev/null
endpoint="http://$(docker port "$name" 9000/tcp)"
curl --silent --fail --retry 60 --retry-delay 1 --retry-all-errors --max-time 5 \
  "$endpoint/health/ready" > /dev/null
curl --silent --show-error --fail --aws-sigv4 aws:amz:us-east-1:s3 \
  --user "$AWS_ACCESS_KEY_ID:$AWS_SECRET_ACCESS_KEY" --request PUT "$endpoint/smoke"
pnpm exec tsx scripts/ci/smoke/s3-lifecycle.mts quarry/quarry "$endpoint"
