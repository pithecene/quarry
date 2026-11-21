# Compatibility fixtures

Each fixture here is a dataset that a released Quarry wrote, checked in as
that release left it. `compat_test.go` opens each one with the current code
and reads, extends, and prunes it. The tests answer one question: does this
release still work on the data that users already have?

| Fixture | Written by | Lode format | Generator | Test |
|---|---|---|---|---|
| `v0.13.7/` | Quarry v0.13.7 on Lode v0.10.0 | 1.0.0 | `gen/` | `TestCompat_v0_13_7_Dataset` |
| `v0.18.0.zip` | Quarry v0.18.0 on Lode v0.17.1 | 1.1.0 | `gen-v0.18.0/` | `TestCompat_v0_18_0_Dataset` |

## Why the data comes from a released Quarry

A test that writes with the current code and then reads with the current
code passes even when the current code misreads older data, because both
sides agree. Only bytes that an older release wrote expose that kind of
drift.

This happened during the move to Lode v0.18. Lode's own tests built format
1.1.0 manifests by hand, and Lode v0.18.0 shipped with a bug: a view anchored
at a format 1.1.0 snapshot lost the manifest's `paths` object, so
`OpenDecompressed` rejected it. Quarry opens every artifact through such a
view. The v0.18.0 fixture failed on that bug, and Lode v0.18.1 fixed it.

Each generator builds against the source of its release tag, not against the
current code. Go cannot fetch a released Quarry by version, because the module
lives in the `quarry/` directory and the tags carry no `quarry/` prefix. So
each generator's doc comment says how to build it from a copy of the tag, and
the fixture is checked in rather than generated during the test.

## Why these releases

- **v0.13.7** is the last release before run records. Its data has format
  1.0.0 manifests, which record no settings. Its test checks that the rebuild
  backfills run records for runs that predate them.
- **v0.18.0** is the last release on Lode v0.17.1, so it writes format 1.1.0,
  the format of every dataset that a current user has. It also writes the
  storage that later releases added: run records, commit index entries,
  artifact Volumes, and sidecar files.

## What each fixture holds

Both fixtures hold the same four runs, so both tests share their checks
(`checkFixtureRuns`, `checkOKRun`, and `pruneFailedRun`):

| Run | Stands for | Checked |
|---|---|---|
| `run-ok-1` | A run that succeeded, with three items, an artifact, a sidecar file, and metrics | State `succeeded`, events in sequence order, metrics, the sidecar, and the artifact's bytes |
| `run-fail-1` | A run that ended with `run_error` | State `failed`, and the prune removes it |
| `run-retry-1` | A retry of `run-fail-1` in job `job-1` | State `succeeded`, attempt 2, its parent run, and its job |
| `run-killed-1` | A run whose process was killed: a start record and no end record | State `unterminated` |

The v0.18.0 test also opens `run-ok-1`'s artifact through its commit index
entry and fails if the reader falls back to reading the whole partition.
That is the read path that the move to Lode v0.18 changed most.

## Why each test ends with a write and a prune

After an upgrade, a dataset holds snapshots of the old format and of the new
one together. Each test writes a new run with the current code into the old
dataset (`appendRun`), prunes the failed run, and lists the runs again. The
v0.18.0 test then reads the artifact again, so that a read across both
formats is checked too.

## Why the v0.18.0 fixture is a zip

Lode keeps each dataset manifest twice by design: a canonical copy and a copy
in its partition. Bosun's duplicates check reports every such pair as copied
code, and it offers no way to exclude test data. The zip keeps the fixture's
files and bytes exactly as Quarry v0.18.0 wrote them, and
`compat_fixture_test.go` unpacks it into a temporary directory for each run.

## Regenerating a fixture

A fixture changes only when its release's storage code is found to differ
from what was checked in. Each generator's doc comment gives the steps.
Every input is fixed, so a new generation differs only in snapshot IDs and
creation times. The tests do not depend on those values.
