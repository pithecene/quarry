# Integration Pattern Example

[`polling-runs.ts`](polling-runs.ts) polls storage for succeeded runs with the SDK's
`listRuns` and `readRun`. Its header states the setup, the S3 change, and what it leaves
to the consumer.

```bash
npx tsx polling-runs.ts ./quarry-data
```

Processing must be idempotent, because a poll can handle a run again.

The [integration guide](../../docs/guides/integration.md) describes this pattern and the
event bus, whose notification carries the locator of its run.
