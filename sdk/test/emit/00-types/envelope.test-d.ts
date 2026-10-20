/**
 * Type-level contract tests for EventEnvelope types.
 *
 * Goal: Ensure envelope types enforce correct structure.
 *
 * Checked at compile time by `vitest --typecheck` (expectTypeOf).
 */
import { expectTypeOf, test } from 'vitest'
import type {
  ArtifactId,
  ArtifactPayload,
  CheckpointId,
  ContractVersion,
  EventEnvelope,
  EventId,
  EventType,
  ItemPayload,
  JobId,
  LogLevel,
  LogPayload,
  PayloadMap,
  RunCompletePayload,
  RunErrorPayload,
  RunId
} from '../../../src/types/events'

test('EventType accepts every event type', () => {
  expectTypeOf<'item'>().toExtend<EventType>()
  expectTypeOf<'artifact'>().toExtend<EventType>()
  expectTypeOf<'checkpoint'>().toExtend<EventType>()
  expectTypeOf<'enqueue'>().toExtend<EventType>()
  expectTypeOf<'rotate_proxy'>().toExtend<EventType>()
  expectTypeOf<'log'>().toExtend<EventType>()
  expectTypeOf<'run_error'>().toExtend<EventType>()
  expectTypeOf<'run_complete'>().toExtend<EventType>()
})

test('PayloadMap maps event types to payloads', () => {
  expectTypeOf<PayloadMap['item']>().toEqualTypeOf<ItemPayload>()
  expectTypeOf<PayloadMap['artifact']>().toEqualTypeOf<ArtifactPayload>()
  expectTypeOf<PayloadMap['log']>().toEqualTypeOf<LogPayload>()
  expectTypeOf<PayloadMap['run_error']>().toEqualTypeOf<RunErrorPayload>()
  expectTypeOf<PayloadMap['run_complete']>().toEqualTypeOf<RunCompletePayload>()
})

test('EventEnvelope narrows type and payload by event type', () => {
  expectTypeOf<EventEnvelope<'item'>['type']>().toEqualTypeOf<'item'>()
  expectTypeOf<EventEnvelope<'item'>['payload']>().toEqualTypeOf<ItemPayload>()
  expectTypeOf<EventEnvelope<'log'>['type']>().toEqualTypeOf<'log'>()
  expectTypeOf<EventEnvelope<'log'>['payload']>().toEqualTypeOf<LogPayload>()
})

test('EventEnvelope carries the envelope fields', () => {
  expectTypeOf<EventEnvelope['contract_version']>().toEqualTypeOf<ContractVersion>()
  expectTypeOf<EventEnvelope['event_id']>().toEqualTypeOf<EventId>()
  expectTypeOf<EventEnvelope['run_id']>().toEqualTypeOf<RunId>()
  expectTypeOf<EventEnvelope['seq']>().toEqualTypeOf<number>()
  expectTypeOf<EventEnvelope['ts']>().toEqualTypeOf<string>()
  expectTypeOf<EventEnvelope['attempt']>().toEqualTypeOf<number>()
})

test('EventEnvelope optional fields', () => {
  expectTypeOf<EventEnvelope['job_id']>().toEqualTypeOf<JobId | undefined>()
  expectTypeOf<EventEnvelope['parent_run_id']>().toEqualTypeOf<RunId | undefined>()
})

test('branded IDs are assignable to string', () => {
  expectTypeOf<RunId>().toExtend<string>()
  expectTypeOf<EventId>().toExtend<string>()
  expectTypeOf<JobId>().toExtend<string>()
  expectTypeOf<ArtifactId>().toExtend<string>()
  expectTypeOf<CheckpointId>().toExtend<string>()
})

test('LogLevel accepts every level', () => {
  expectTypeOf<'debug'>().toExtend<LogLevel>()
  expectTypeOf<'info'>().toExtend<LogLevel>()
  expectTypeOf<'warn'>().toExtend<LogLevel>()
  expectTypeOf<'error'>().toExtend<LogLevel>()
})
