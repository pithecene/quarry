# Examples

Small, deterministic examples for Quarry. These use local fixtures and
avoid external network dependencies.

## demo.ts

Minimal item emission:
- [`examples/demo.ts`](demo.ts)

## static-html-list

Parses a local HTML fixture and emits items:
- [`examples/static-html-list/`](static-html-list/)

## toy-pagination

Paginates across two local fixtures:
- [`examples/toy-pagination/`](toy-pagination/)

## artifact-snapshot

Renders a local fixture and emits a screenshot artifact:
- [`examples/artifact-snapshot/`](artifact-snapshot/)

## intentional-failure

Tests the error path by throwing after one item:
- [`examples/intentional-failure/`](intentional-failure/)

## fan-out-chain

Demonstrates fan-out derived work execution (list → detail):
- [`examples/fan-out-chain/`](fan-out-chain/)

## integration-patterns

Conceptual examples for downstream event-bus and polling integration:
- [`examples/integration-patterns/`](integration-patterns/)
