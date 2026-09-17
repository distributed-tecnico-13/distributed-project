# DAD Project — GitHub Issue Backlog for the 30 September Checkpoint

## Scope

This backlog is focused on the **30 September checkpoint**.

For a group targeting **Vanilla or above**, the checkpoint goal should be:

- **Step 1 — Complete the baseline**
- **Step 2 — Multi-Paxos**

Steps 3–5 should be left for the final deadline unless the group finishes the checkpoint work early.

---

# Workload Scale

The estimates below are approximate **active development time per person**, including implementation, local testing, and basic code cleanup.

| Size | Estimated effort | Meaning |
|---|---:|---|
| **XS** | 0.5–1 h | Very small change |
| **S** | 1–3 h | Small, isolated task |
| **M** | 3–6 h | Moderate task, some debugging |
| **L** | 6–10 h | Significant implementation/reasoning |
| **XL** | 10–16 h | Major architectural change |

> These are estimates, not deadlines. Paxos/concurrency bugs can easily make an issue take longer than expected.

---

# Topic 0 — Baseline Understanding & Project Infrastructure

These issues should be done first because they reduce the risk of implementing the algorithm incorrectly.

---

## Issue 1 — Document the end-to-end request flow

**Labels:** `step-1`, `documentation`, `baseline`, `size:S`  
**Workload:** **S — 2–3 h**  
**Priority:** High

### Goal

Document how a client request travels through the system.

Expected flow:

```text
Client
  ↓
DidaTradeMainServiceImpl
  ↓
RequestHistory
  ↓
MainLoop
  ↓
Paxos Phase 1 / Phase 2
  ↓
PaxosLog
  ↓
TradeManager
  ↓
Client response
```

### Tasks

- Trace one `buy` request through the code.
- Identify where the request ID is generated.
- Identify where the request becomes pending.
- Identify where Paxos decides its log position.
- Identify where the state machine executes it.
- Identify when the client finally receives a response.
- Add a small Markdown document or comments for the team.

### Acceptance criteria

- Every group member can explain the full path of a client request.
- The relationship between `RequestHistory`, `MainLoop`, `PaxosLog`, and `TradeManager` is documented.

### Dependencies

None.

---

## Issue 2 — Validate startup/build scripts and local environment

**Labels:** `infrastructure`, `scripts`, `size:S`  
**Workload:** **S — 1–2 h**  
**Priority:** Medium

### Goal

Ensure all three group members can build and run the exact same baseline.

### Tasks

- Verify Java 22.
- Verify Maven build.
- Verify `mvn clean install`.
- Verify startup scripts.
- Fix local path inconsistencies if they exist.
- Confirm Schedule A launches the intended number of replicas.
- Document the commands required to start:
  - servers;
  - client;
  - console.

### Acceptance criteria

- A fresh clone builds successfully.
- Every group member can launch Schedule A without manual debugging.
- The startup procedure is documented.

### Dependencies

None.

---

# Topic 1 — Debug / Failure Infrastructure

This is a core requirement of Step 1.

The implementation should ideally centralize failure behavior instead of duplicating debug checks in every RPC method.

---

## Issue 3 — Add centralized server debug-state control

**Labels:** `step-1`, `debug`, `server`, `size:M`  
**Workload:** **M — 3–5 h**  
**Priority:** High

### Goal

Create a single mechanism in the server state that controls:

- frozen state;
- slow-mode state;
- crash handling.

### Tasks

- Define the server debug states/modes.
- Make debug state thread-safe.
- Add helper methods such as:
  - `beforeClientOrServerRequest()`;
  - `freeze()`;
  - `unfreeze()`;
  - `enableSlowMode()`;
  - `disableSlowMode()`.
- Ensure console/master RPCs can bypass normal request blocking.

### Acceptance criteria

- Debug behavior is controlled from one central place.
- Normal RPC services can use the same mechanism.
- Master/console RPCs are not unintentionally blocked.

### Dependencies

Issue 1 recommended.

---

## Issue 4 — Implement `freeze` / `un-freeze`

**Labels:** `step-1`, `debug`, `freeze`, `size:M`  
**Workload:** **M — 4–6 h**  
**Priority:** High

### Goal

Allow a replica to stop processing client/server requests without losing them.

### Required behavior

```text
freeze
   ↓
incoming client/server RPCs block
   ↓
requests remain pending
   ↓
un-freeze
   ↓
blocked requests continue
```

### Tasks

- Block client RPC processing while frozen.
- Block server-to-server Paxos RPC processing while frozen.
- Do **not** discard requests.
- Wake all blocked requests after `un-freeze`.
- Ensure synchronization is correct.

### Acceptance criteria

- Requests arriving during freeze do not complete.
- Requests arriving during freeze are not lost.
- After unfreeze, they resume.
- Console commands continue working while the server is frozen.

### Dependencies

Issue 3.

---

## Issue 5 — Implement `slow-mode-on` / `slow-mode-off`

**Labels:** `step-1`, `debug`, `latency`, `size:S`  
**Workload:** **S — 2–3 h**  
**Priority:** High

### Goal

Add random processing delays to client/server requests.

### Tasks

- Define a reasonable random delay range.
- Apply delay before normal client RPC processing.
- Apply delay before Paxos/server RPC processing.
- Do not slow down console/master control messages.
- Ensure disabling slow mode removes the delay.

### Acceptance criteria

- Slow mode visibly changes request latency.
- Requests still eventually complete.
- `slow-mode-off` restores normal behavior.
- Console control remains responsive.

### Dependencies

Issue 3.

---

## Issue 6 — Implement `crash`

**Labels:** `step-1`, `debug`, `crash`, `size:XS`  
**Workload:** **XS — 0.5–1 h**  
**Priority:** High

### Goal

Terminate a selected server when the crash mode is triggered.

### Tasks

- Implement server shutdown/termination.
- Ensure the behavior matches the expected console command.

### Acceptance criteria

- The selected process exits.
- Other replicas continue running.

### Dependencies

Issue 3.

---

## Issue 7 — Verify MasterService remains usable under all debug modes

**Labels:** `step-1`, `debug`, `testing`, `size:S`  
**Workload:** **S — 1–2 h**  
**Priority:** High

### Goal

Ensure administrative commands remain available even when normal request processing is disrupted.

### Test

While a server is frozen:

```text
debug freeze <server>
debug un-freeze <server>
```

must still work.

### Acceptance criteria

- `un-freeze` can always reach a frozen server.
- Slow mode does not delay administrative commands.
- Debug commands remain usable for failure experiments.

### Dependencies

Issues 4–6.

---

# Topic 2 — Correct Paxos Phase 1

This is the most important Step 1 correctness work.

---

## Issue 8 — Build a reproducible execution showing why the baseline Phase 1 is wrong

**Labels:** `step-1`, `paxos`, `testing`, `size:M`  
**Workload:** **M — 3–5 h**  
**Priority:** High

### Goal

Use delays/freezes/ballot changes to show that `PhaseOneBogusProcessor` is unsafe.

### Tasks

- Understand why the existing processor finishes after one response.
- Construct a deterministic or mostly deterministic failure scenario.
- Record:
  - ballot numbers;
  - accepted values;
  - responding replicas;
  - chosen value.
- Explain the safety violation.

### Acceptance criteria

The team can demonstrate an execution where:

1. a previous value has been accepted;
2. the new proposer receives an incomplete Phase 1 view;
3. the bogus processor proceeds too early;
4. the proposer can choose the wrong value.

### Dependencies

Issues 4–7 recommended.

---

## Issue 9 — Implement quorum tracking for Phase 1

**Labels:** `step-1`, `paxos`, `phase-1`, `size:L`  
**Workload:** **L — 6–8 h**  
**Priority:** Critical

### Goal

Replace the one-response behavior with correct quorum collection.

### Tasks

- Create/complete `PhaseOneResponseProcessor`.
- Count valid responses.
- Determine quorum size from the active configuration.
- Do not finish after one response.
- Handle asynchronous replies correctly.
- Handle duplicate responses safely if relevant.

### Acceptance criteria

For Schedule A:

```text
3 acceptors
quorum = 2
```

The proposer does not continue before receiving the required quorum.

### Dependencies

Issue 1.  
Issue 8 strongly recommended.

---

## Issue 10 — Implement highest accepted-ballot value selection

**Labels:** `step-1`, `paxos`, `phase-1`, `safety`, `size:M`  
**Workload:** **M — 4–6 h**  
**Priority:** Critical

### Goal

Choose the Paxos-safe value after Phase 1.

### Required rule

Among Phase 1 responses:

- if no acceptor has accepted a previous value → propose the leader's new value;
- otherwise → propose the value associated with the **highest accepted ballot**.

### Example

```text
S0 → accepted ballot = -1
S1 → accepted ballot = 4, value = 301
```

The proposer must choose:

```text
301
```

even if it originally wanted to propose another request.

### Acceptance criteria

- Highest accepted ballot is tracked correctly.
- Its corresponding value is returned.
- Empty/unaccepted responses do not overwrite a valid accepted value.

### Dependencies

Issue 9.

---

## Issue 11 — Handle higher-ballot/rejected Phase 1 responses

**Labels:** `step-1`, `paxos`, `ballots`, `size:M`  
**Workload:** **M — 3–5 h**  
**Priority:** High

### Goal

Handle situations where an acceptor has already observed a higher ballot than the proposer.

### Tasks

- Inspect existing reply fields such as `maxballot`.
- Detect when the current proposer is stale.
- Prevent unsafe continuation.
- Propagate the observed higher ballot to the relevant server state if required by the design.

### Acceptance criteria

- A proposer cannot continue incorrectly after learning that its ballot is obsolete.
- Behavior is deterministic and documented.

### Dependencies

Issue 9.

---

## Issue 12 — Integrate correct Phase 1 processor into `MainLoop`

**Labels:** `step-1`, `paxos`, `integration`, `size:M`  
**Workload:** **M — 3–5 h**  
**Priority:** Critical

### Goal

Remove the baseline bogus behavior from normal execution.

### Tasks

Replace:

```java
PhaseOneBogusProcessor
```

with the correct processor.

Ensure the selected value is passed correctly to Phase 2.

### Acceptance criteria

- Normal server execution no longer uses the bogus processor.
- The build passes.
- Existing client operations still work.
- Failure scenarios do not violate the corrected Phase 1 logic.

### Dependencies

Issues 9–11.

---

# Topic 3 — Step 1 Tests

---

## Issue 13 — Unit tests for Phase 1 response processing

**Labels:** `step-1`, `testing`, `paxos`, `size:M`  
**Workload:** **M — 4–6 h**  
**Priority:** High

### Test cases

At minimum:

1. no previously accepted values;
2. one previously accepted value;
3. several accepted values with different ballots;
4. highest ballot is not the last response received;
5. fewer than quorum responses;
6. quorum reached;
7. response indicates higher ballot.

### Acceptance criteria

- Tests fail against the bogus implementation where appropriate.
- Tests pass against the corrected implementation.

### Dependencies

Issues 9–11.

---

## Issue 14 — End-to-end Step 1 failure tests

**Labels:** `step-1`, `testing`, `integration`, `size:M`  
**Workload:** **M — 4–6 h**  
**Priority:** High

### Scenarios

- Freeze one replica.
- Continue with quorum.
- Unfreeze replica.
- Slow one replica.
- Crash one replica.
- Trigger ballot changes.
- Verify replicas retain a consistent state.

### Acceptance criteria

- No committed operation disappears.
- Remaining quorum can make progress where expected.
- Recovered/unfrozen replica behaves consistently with the protocol.

### Dependencies

Issues 4–12.

---

# Topic 4 — Multi-Paxos / Concurrent Paxos Instances

This is the main Step 2 work.

This part should begin only once Step 1 is stable.

---

## Issue 15 — Design Multi-Paxos instance lifecycle

**Labels:** `step-2`, `multi-paxos`, `design`, `size:M`  
**Workload:** **M — 3–5 h**  
**Priority:** High

### Goal

Agree on the concurrency design before modifying `MainLoop`.

### Questions to answer

- Who allocates the next log instance?
- How many instances may be in flight?
- Where is per-instance proposal state stored?
- How does a proposer learn that an instance is decided?
- How are completed instances removed/cleaned up?
- How does state-machine execution remain ordered?

### Deliverable

A short design note/diagram.

### Acceptance criteria

- All three group members agree on the design.
- The implementation responsibilities for Issues 16–19 are clear.

### Dependencies

Step 1 complete.

---

## Issue 16 — Refactor proposal processing to allow multiple instances in flight

**Labels:** `step-2`, `multi-paxos`, `concurrency`, `size:XL`  
**Workload:** **XL — 10–14 h**  
**Priority:** Critical

### Goal

Remove the current behavior where instance `n+1` waits for instance `n` to finish before starting.

### Desired behavior

```text
instance 10 ────────────────►
instance 11    ────────────────►
instance 12       ────────────────►
instance 13          ────────────────►
```

### Tasks

- Refactor `MainLoop`.
- Separate proposal initiation from state-machine execution.
- Maintain per-instance state.
- Ensure concurrency does not corrupt request assignment.

### Acceptance criteria

- Multiple Paxos instances can be actively progressing at once.
- Starting instance `n+1` does not require instance `n` to already be decided.

### Dependencies

Issue 15.

---

## Issue 17 — Track decided instances independently of execution

**Labels:** `step-2`, `multi-paxos`, `log`, `size:L`  
**Workload:** **L — 6–8 h**  
**Priority:** Critical

### Goal

Allow Paxos instances to become decided in arbitrary order.

Example:

```text
instance 12 decided
instance 10 decided
instance 11 decided
```

must be representable without immediately executing 12.

### Tasks

- Mark instances as decided.
- Preserve their chosen request/value.
- Keep them pending for ordered execution.

### Acceptance criteria

- A higher-numbered decided instance can safely wait for earlier instances.

### Dependencies

Issue 16.

---

## Issue 18 — Enforce strict state-machine execution order

**Labels:** `step-2`, `multi-paxos`, `state-machine`, `size:L`  
**Workload:** **L — 6–8 h**  
**Priority:** Critical

### Goal

Execute commands strictly by Paxos log index.

### Example

Even if decisions occur:

```text
12 → 10 → 11
```

execution must be:

```text
10 → 11 → 12
```

### Tasks

- Maintain `next_to_execute`.
- Check whether the next instance is decided.
- Execute all contiguous decided entries.
- Do not skip gaps.
- Complete the correct `RequestRecord` after execution.

### Acceptance criteria

- `TradeManager` observes identical command order on every replica.
- Out-of-order decisions never produce out-of-order execution.

### Dependencies

Issue 17.

---

## Issue 19 — Audit synchronization/thread safety for concurrent instances

**Labels:** `step-2`, `concurrency`, `thread-safety`, `size:L`  
**Workload:** **L — 6–9 h**  
**Priority:** Critical

### Goal

Ensure the new concurrency does not introduce races.

### Components to inspect

- `PaxosLog`
- `PaxosInstance`
- `RequestHistory`
- `MainLoop`
- response processors
- shared ballot/configuration state

### Tasks

Look for:

- unsynchronized maps;
- check-then-act races;
- lost wakeups;
- duplicate execution;
- duplicate decisions;
- inconsistent counters.

### Acceptance criteria

- Shared state has a clear synchronization strategy.
- Stress tests do not produce obvious races or duplicated execution.

### Dependencies

Issues 16–18.

---

# Topic 5 — Step 2 Testing / Stress Testing

---

## Issue 20 — Test out-of-order Paxos decisions

**Labels:** `step-2`, `testing`, `multi-paxos`, `size:M`  
**Workload:** **M — 4–6 h**  
**Priority:** High

### Goal

Force later instances to finish before earlier ones.

### Possible tools

- slow mode;
- freeze;
- artificial test delays;
- multiple clients.

### Acceptance criteria

- Decisions may complete out of order.
- Execution remains strictly ordered.
- Replicas finish with identical state.

### Dependencies

Issues 16–19.

---

## Issue 21 — Multi-client stress test

**Labels:** `step-2`, `testing`, `stress`, `size:M`  
**Workload:** **M — 4–6 h**  
**Priority:** High

### Goal

Generate many concurrent requests from multiple clients.

### Tasks

- Start multiple clients.
- Generate repeated `buy`/`sell`/other commands.
- Compare final server states/logs.
- Look for lost or duplicated requests.

### Acceptance criteria

- No request is executed twice.
- No decided request disappears.
- All replicas reach equivalent state.

### Dependencies

Issues 16–19.

---

## Issue 22 — Stress test Multi-Paxos with failures

**Labels:** `step-2`, `testing`, `failure`, `size:L`  
**Workload:** **L — 6–8 h**  
**Priority:** High

### Scenarios

- multiple clients + one slow server;
- multiple clients + one frozen server;
- unfreeze while proposals are active;
- crash a non-essential replica;
- ballot changes while multiple instances exist.

### Acceptance criteria

- Safety is preserved.
- Progress occurs whenever the active configuration still permits it.
- No out-of-order state-machine execution occurs.

### Dependencies

Issues 16–21.

---

# Topic 6 — Checkpoint Integration & Submission Readiness

---

## Issue 23 — Add structured debug/log output for Paxos experiments

**Labels:** `logging`, `testing`, `checkpoint`, `size:S`  
**Workload:** **S — 2–3 h**  
**Priority:** Medium

### Goal

Make protocol executions understandable during the checkpoint/demo.

Useful fields:

```text
server
instance
ballot
phase
request/value
accepted ballot
decision
execution index
```

### Acceptance criteria

Logs allow the group to reconstruct:

- which server proposed;
- which value was selected;
- which instance was decided;
- when it was executed.

### Dependencies

Can be done throughout development.

---

## Issue 24 — Final checkpoint regression test

**Labels:** `checkpoint`, `testing`, `integration`, `size:L`  
**Workload:** **L — 5–8 h**  
**Priority:** Critical

### Test checklist

- Fresh clone builds.
- Schedule A starts.
- Client operations work.
- Replicas remain consistent.
- Crash works.
- Freeze works.
- Unfreeze works.
- Slow mode works.
- Correct Phase 1 uses quorum.
- Previously accepted values are preserved.
- Multiple Paxos instances run concurrently.
- Out-of-order decisions execute in order.
- Multi-client workload works.
- Failure scenarios work.

### Acceptance criteria

Every required checkpoint feature has at least one reproducible test/demo.

### Dependencies

All checkpoint implementation issues.

---

# Estimated Total Workload

Approximate total active workload:

| Topic | Estimated total |
|---|---:|
| Baseline & infrastructure | 3–5 h |
| Debug infrastructure | 10–17 h |
| Correct Phase 1 | 16–24 h |
| Step 1 testing | 8–12 h |
| Multi-Paxos implementation | 31–44 h |
| Step 2 testing | 14–20 h |
| Integration/logging | 7–11 h |
| **Total** | **89–133 person-hours** |

With **3 people**, this is approximately:

```text
30–45 hours per person
```

if the work is distributed evenly.

This includes debugging/testing time, not only writing code.

---

# Suggested Distribution Across 3 People

Do not distribute based purely on issue count. Use effort and complexity.

A possible initial split:

## Person A — Failure/debug infrastructure

Primary issues:

- #3 Centralized debug-state control — M
- #4 Freeze/unfreeze — M
- #6 Crash — XS
- #7 MasterService debug-mode verification — S
- #14 Step 1 integration tests — M

Approximate initial workload:

```text
13–20 h
```

---

## Person B — Paxos Phase 1 correctness

Primary issues:

- #8 Demonstrate bogus Phase 1 — M
- #9 Phase 1 quorum processor — L
- #10 Highest accepted-ballot selection — M
- #11 Higher-ballot handling — M
- #12 MainLoop integration — M

Approximate initial workload:

```text
19–29 h
```

This is the most algorithmically sensitive area, so the other two members should review these PRs.

---

## Person C — Infrastructure/testing + supporting implementation

Primary issues:

- #1 Document baseline flow — S
- #2 Build/startup environment — S
- #5 Slow mode — S
- #13 Phase 1 unit tests — M
- #23 Structured logging — S

Approximate initial workload:

```text
11–17 h
```

Person C can then move early into Step 2 once these are complete.

---

# Suggested Step 2 Redistribution

After Step 1 is merged, redistribute work rather than keeping fixed ownership.

## Person A

- #16 Concurrent Paxos instances — shared/lead
- #21 Multi-client stress tests

## Person B

- #17 Decided-instance tracking
- #19 Thread-safety audit

## Person C

- #18 Ordered state-machine execution
- #20 Out-of-order decision tests

All three should contribute to:

- #15 Multi-Paxos design
- #22 Failure stress tests
- #24 Final checkpoint regression

---

# Recommended GitHub Labels

```text
step-1
step-2

paxos
multi-paxos
debug
testing
integration
concurrency
infrastructure
documentation
logging

priority:critical
priority:high
priority:medium

size:XS
size:S
size:M
size:L
size:XL
```

---

# Recommended Milestone

Create:

```text
Checkpoint — 30 September
```

and assign Issues **1–24** that you decide are required for your chosen target.

For a Vanilla/Premium/Ultimate target, Step 1 and Step 2 should both be included in this milestone.

---

# Recommended PR Workflow

For every issue:

```text
GitHub Issue
     ↓
feature branch
     ↓
implementation
     ↓
tests
     ↓
Pull Request
     ↓
review by ≥1 teammate
     ↓
merge
```

Suggested branch names:

```text
issue/4-freeze-unfreeze
issue/9-phase-one-quorum
issue/18-ordered-execution
```

For Paxos safety/concurrency PRs, preferably require **both other members to review**.

---

# Definition of Done for an Issue

An issue is not complete just because the code compiles.

Use this checklist:

- [ ] Implementation complete
- [ ] Project builds
- [ ] Relevant test/manual scenario passes
- [ ] No known regression
- [ ] Acceptance criteria satisfied
- [ ] Code reviewed by teammate
- [ ] PR merged
- [ ] Issue closed

---

# Checkpoint Definition of Done — 30 September

For Step 1 + Step 2, the group should be able to demonstrate:

- [ ] Baseline system starts correctly
- [ ] Normal replicated operations work
- [ ] `crash` works
- [ ] `freeze` works
- [ ] `un-freeze` works
- [ ] frozen requests are not discarded
- [ ] `slow-mode-on` works
- [ ] `slow-mode-off` works
- [ ] console control remains available during failures
- [ ] original bogus Phase 1 behavior can be explained
- [ ] correct Phase 1 waits for quorum
- [ ] highest previously accepted ballot/value is preserved
- [ ] stale/higher ballot behavior is handled
- [ ] multiple Paxos instances can be in flight
- [ ] Paxos decisions may finish out of order
- [ ] state-machine execution remains strictly ordered
- [ ] concurrent multi-client tests succeed
- [ ] replicas converge to the same state
- [ ] checkpoint regression scenario is reproducible
