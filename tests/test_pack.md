# DidaTrade checkpoint test pack

This pack is designed to be copied directly into the repository root. All Java files are already under the Maven test-source directories they belong to, and the process-level tests live under `tests/`.

## 1. Fast/default unit tests

These files are named `*Test.java`, so the normal project test command will discover them automatically.

### `src/server/src/test/java/didatrade/server/MultiPaxosPrepareTest.java`

Tests the teacher-required stable-leader Multi-Paxos behavior around Phase 1:

- a successful `prepare(ballot)` sends Phase 1 RPCs once and reuses that prepare on the same ballot;
- changing to a new ballot requires a new Phase 1;
- one failed acceptor RPC is tolerated when a quorum still promises;
- Phase 1 fails when fewer than a quorum reply successfully;
- a higher-ballot rejection aborts preparation, updates `current_ballot`, and does not mark the old ballot prepared;
- Phase 1 merges accepted history independently per instance, keeps the highest accepted ballot for each slot, and fills gaps with NOOP candidates.

The test uses three small real gRPC acceptor endpoints, but does not launch full DidaTrade replicas.

### `src/server/src/test/java/didatrade/server/PaxosReproposalTest.java`

Tests reproposal invariants:

- a decided slot can be claimed once in a newer ballot for the professor-required reproposal;
- it cannot be proposed twice in the same ballot;
- a decided slot always exposes its chosen command as `getValueToPropose()` rather than a stale local candidate;
- Phase 2 retries in the same ballot stay locked to the first value;
- attempting to restart an older Phase 2 ballot is rejected locally.

### `src/server/src/test/java/didatrade/server/PaxosServiceSnapshotTest.java`

Tests the real Phase 1 service handler:

- Phase 1 replies contain every locally accepted slot with its accepted value/ballot;
- merely proposed/unaccepted local candidates are not exposed as accepted history;
- a lower prepare is rejected and reports the current higher ballot.

The existing `PhaseOneAtomicityTest` already covers the more subtle snapshot+promise race.

### `src/server/src/test/java/didatrade/server/ServerStateBallotInvariantTest.java`

Tests server-wide ballot state:

- ballot numbers never move backwards;
- only the leader of the exact current ballot may propose;
- lower prepares cannot undo a higher promise;
- `completed_ballot` follows the highest ballot among all decided log entries, including entries past a hole.

### `src/server/src/test/java/didatrade/server/RequestHistoryInvariantTest.java`

Tests request-history bookkeeping:

- internal sentinel IDs (`-1`, `-2`) cannot enter client pending history;
- processed requests leave `pending` and remain discoverable through `processed/getIfExists`;
- allocation can skip a set of already-owned request IDs.

It also contains one **disabled diagnostic regression** for duplicate request IDs. The current `addToPending()` overwrites a pending record with the same ID, which could strand an RPC thread waiting on the old `RequestRecord`. Enable this test only if the group decides duplicate/retried client requests must be idempotent at this layer.

### `src/util/src/test/java/didatrade/util/GenericResponseCollectorConcurrencyTest.java`

Concurrency-tests the callback collector:

- once a processor declares completion, concurrent late callbacks do not invoke the processor again;
- a concurrent mix of successful callbacks and RPC failures still releases `waitUntilDone()` after every expected callback has arrived.

## 2. Deliberately disabled protocol diagnostic

### `src/server/src/test/java/didatrade/server/PhaseOneRecoveryCollisionTest.java`

This is the exact issue currently being discussed with Samuel. It encodes the invariant:

> if successful Phase 1 says slot `s` must use value `X`, local request-allocation bookkeeping must not silently veto `X` because the same request ID is currently attached to another stale local slot.

It is marked `@Disabled` so it does not break the normal suite while the team settles the intended `PaxosLog.adopt()` semantics. If Samuel agrees with the review concern, remove `@Disabled`, observe the failure, fix the recovery path, and keep this as the regression test.

## 3. Real-TCP integration tests

### `src/server/src/test/java/didatrade/server/Integration_MultiPaxosClusterTest.java`

This class launches three real DidaTrade states and three real gRPC servers over loopback TCP. The name ends in `Test`, so the normal test command runs it with the unit tests. `just test-integration` runs every class named `Integration_*`.

It tests:

- 12 concurrent broadcast client requests and convergence across all three replicas;
- progress with one follower frozen, then catch-up after unfreeze;
- a real ballot/leader change from ballot 0 to ballot 1, successful Phase 1 by the new leader, and reproposal of an already chosen slot without changing its value.

Run it with:

```bash
bash tests/run-integration.sh
```

## 4. Process-level E2E tests

These use the project's existing Maven launch mechanism and the existing interactive app/console. They start three actual server processes and save all logs under `logs/e2e/<run-id>/`.

- `tests/e2e/01_smoke_and_convergence.sh`: basic command sequence, all three processes alive, final observable dump equal across replicas.
- `tests/e2e/02_multi_client_stress.sh`: six clients concurrently submit independent workloads; all finish and replicas converge.
- `tests/e2e/03_freeze_unfreeze.sh`: freezes replica 1 through the real console RPC, checks quorum progress, unfreezes it, then checks convergence.
- `tests/e2e/04_slow_follower_stress.sh`: enables real debug slow mode on one follower while several clients run concurrently, then disables it and checks convergence.
- `tests/e2e/05_crash_survival.sh`: sends real debug `crash` to replica 1, verifies the process/port actually disappear, then verifies replicas 0 and 2 still make progress. This is the process-level crash test that unit tests cannot safely provide because debug crash calls `System.exit(0)`.
- `tests/e2e/06_ballot_change_reproposal.sh`: first chooses work in ballot 0, then asks replica 1 to lead ballot 1, waits for the ballot control RPC to complete, submits more work, and checks convergence.
- `tests/e2e/07_out_of_order_stress.sh`: uses a slow replica plus eight concurrent clients to vary decision timing, checks final convergence, and reports whether learner logs visibly showed a decreasing quorum-decision order. The deterministic rule “decisions may finish out of order, execution may not” is already covered by the existing `OrderedPaxosExecutorTest`.
- `tests/e2e/08_leader_crash_and_recovery.sh`: crashes the ballot-0 leader, elects replica 1 in ballot 1 with only two survivors, verifies recovery/reproposal of prior chosen work, and then verifies new progress.
- `tests/e2e/09_multiple_ballot_transitions.sh`: rotates leadership across ballots 0 -> 1 -> 2 -> 3 with new work between transitions and checks final convergence.

Run one test, for example:

```bash
bash tests/e2e/05_crash_survival.sh
```

Run the whole E2E suite:

```bash
bash tests/e2e/run_all.sh
```

`run_all.sh` builds once and then runs the scenarios sequentially.

## Recommended checkpoint order

1. `just test` — all normal unit tests, including the new fast tests.
2. `bash tests/run-integration.sh` — the real-TCP in-JVM cluster scenarios.
3. Run `01`, `03`, `05`, and `06` individually first because they cover the most important checkpoint behavior: baseline operation, freeze/unfreeze, actual crash, and leader/ballot change.
4. Run `02`, `04`, and `07` repeatedly for stress/timing variation.
5. Finally run `bash tests/e2e/run_all.sh` from a clean checkout before the checkpoint demo.

## Important note about validation of this pack

The shell scripts were syntax-checked with `bash -n` in the generation environment. I could not execute the Java/Maven test suite here because the sandbox has Maven/JDK binaries but does not have the project's Maven dependency cache, and outbound Maven dependency resolution is unavailable. Therefore, run `just test` immediately after copying the pack into your real project; if the branch changed after the uploaded ZIP, compile errors will identify API drift quickly.
