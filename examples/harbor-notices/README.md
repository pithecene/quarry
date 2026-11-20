# Harbor Notices (Fan-Out Adaptation)

This example is the exact harbor-notice adaptation of [`fan-out-chain`](../fan-out-chain/) presented in the Quarry run inspector on [pithecene.sh/foundations/quarry](https://pithecene.sh/foundations/quarry).

The root script ([`listing.ts`](listing.ts)) reads [`fixtures/index.html`](fixtures/index.html) and enqueues one child run ([`notice.ts`](notice.ts)) per notice (`41/26`, `42/26`, and `43/26`). Under `--policy strict`, Lode commits each emitted event as its own snapshot under `source=harbor/category=default`.

## Run from a Source Checkout

Build Quarry and run the root script against a retained filesystem directory (`./harbor-data` stays on disk after the run so you can inspect the stored runs, events, and Lode manifests):

```bash
pnpm install
task build
mkdir -p ./harbor-data

./quarry/quarry run \
  --executor ./executor-node/dist/bin/executor.js \
  --script ./examples/harbor-notices/listing.ts \
  --run-id notices-001 \
  --source harbor \
  --category default \
  --policy strict \
  --depth 1 \
  --max-runs 10 \
  --parallel 3 \
  --storage-backend fs \
  --storage-path ./harbor-data
```

## Inspect Stored Output

List the root and child runs recorded in `./harbor-data`:

```bash
./quarry/quarry list runs \
  --storage-backend fs \
  --storage-path ./harbor-data \
  --source harbor \
  --format json
```

Read the events of the root run or any child run (replace `<day>` and `<run-id>` with values from `list runs`):

```bash
./quarry/quarry list events \
  --storage-backend fs \
  --storage-path ./harbor-data \
  --source harbor \
  --category default \
  --day <day> \
  --run-id <run-id> \
  --format json
```
