/**
 * Event-bus pattern: TypeScript handler for Quarry run completion events.
 *
 * This example demonstrates an idempotent handler that processes run completion
 * events from an event bus (SNS/SQS, Kafka, etc.). The handler:
 *
 * 1. Validates the event payload
 * 2. Checks if the run has already been processed (idempotency)
 * 3. Reads the run's events through the notification's locator
 * 4. Transforms and loads data to target system
 * 5. Marks the run as processed
 *
 * This is a conceptual example - adapt to your infrastructure.
 */
import { type NotificationLocator, readRun } from '@pithecene/quarry-sdk'

// The fields of Quarry's run_completed notification that this handler uses.
type RunCompletedEvent = {
  event_type: 'run_completed'
  run_id: string
  source: string
  category: string
  day: string
  outcome: string
  locator: NotificationLocator
  timestamp: string
}

// Simulated processed runs store (use a real database in production)
const processedRuns = new Set<string>()

/**
 * Main handler function for run completion events.
 * Designed to be idempotent - safe to call multiple times with the same event.
 */
export async function handleRunCompleted(event: RunCompletedEvent): Promise<void> {
  console.log(`[handler] Received event for run: ${event.run_id}`)

  // 1. Validate event
  if (event.event_type !== 'run_completed') {
    console.log(`[handler] Ignoring non-completion event: ${event.event_type}`)
    return
  }

  // 2. Check idempotency - skip if already processed
  if (processedRuns.has(event.run_id)) {
    console.log(`[handler] Run ${event.run_id} already processed, skipping`)
    return
  }

  // 3. Skip failed runs (or handle differently based on your needs)
  if (event.outcome !== 'success') {
    console.log(`[handler] Run ${event.run_id} failed, skipping processing`)
    // Optionally: send to dead letter queue, alert, etc.
    return
  }

  try {
    // 4. Read and process events from storage
    console.log(`[handler] Processing run ${event.run_id}`)
    await processRunData(event)

    // 5. Mark as processed (after successful processing)
    processedRuns.add(event.run_id)
    console.log(`[handler] Successfully processed run ${event.run_id}`)
  } catch (error) {
    console.error(`[handler] Failed to process run ${event.run_id}:`, error)
    // In production: retry with backoff, send to DLQ, etc.
    throw error
  }
}

/**
 * Process run data from storage.
 * This is where you'd read events/artifacts and transform/load them.
 */
async function processRunData(event: RunCompletedEvent): Promise<void> {
  const { events } = await readRun(event.locator.run, { storage: event.locator.storage })
  const items = events.filter((stored) => stored.type === 'item')

  for (const item of items) {
    await transformAndLoad(item, event)
  }

  console.log(`[handler] Processed ${items.length} items from run ${event.run_id}`)
}

/**
 * Transform and load a single item to target system.
 * Replace with actual ETL logic.
 */
async function transformAndLoad(item: unknown, event: RunCompletedEvent): Promise<void> {
  // Example transformation
  const transformed = {
    ...(item as object),
    _source: event.source,
    _run_id: event.run_id,
    _processed_at: new Date().toISOString()
  }

  // Example: Insert into database, send to API, etc.
  console.log(`[handler] Transformed item:`, transformed)
}

// Example: AWS Lambda handler wrapper
export async function lambdaHandler(sqsEvent: { Records: Array<{ body: string }> }): Promise<void> {
  for (const record of sqsEvent.Records) {
    // SNS wraps the message in another layer
    const snsMessage = JSON.parse(record.body)
    const event: RunCompletedEvent = JSON.parse(snsMessage.Message)
    await handleRunCompleted(event)
  }
}

// Example: Direct invocation for testing
if (import.meta.url === `file://${process.argv[1]}`) {
  const testEvent: RunCompletedEvent = {
    event_type: 'run_completed',
    run_id: 'test-run-001',
    source: 'demo',
    category: 'default',
    day: '2026-02-04',
    outcome: 'success',
    locator: {
      run: { source: 'demo', category: 'default', day: '2026-02-04', runId: 'test-run-001' },
      storage: { backend: 'fs', path: './quarry-data', dataset: 'quarry' }
    },
    timestamp: new Date().toISOString()
  }

  handleRunCompleted(testEvent)
    .then(() => console.log('Handler completed'))
    .catch((err) => console.error('Handler failed:', err))
}
