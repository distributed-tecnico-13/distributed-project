# Design overview: Steps 1–2

## Step 1 — Completing Paxos

The console `setdebug` RPC controls a replica. Mode 1 crashes, 2 freezes request handling until mode 3, and 4 adds random delay until mode 5. Mode 0 clears them. Freeze blocks the request entry points, not the console, so a frozen server can still be unfrozen. A crash waits until the debug RPC has finished.

Phase 1 now succeeds only with a quorum of distinct acceptors.

## Step 2 — Multi-Paxos

For a leader's current ballot, `PaxosProposer.prepare` sends **one global Phase 1**
request to the configured acceptors. Their replies contain accepted log entries, not
just one slot. `PhaseOneResponseProcessor` merges those histories independently by
instance, keeping the value with the highest accepted ballot. The proposer adopts
those recovered values into `PaxosLog` **before** scheduling local client work.
`PaxosLog.adopt()` can replace a stale local candidate, including with a recovered
NOOP. By contrast, `fillNoop()` only speculatively fills gaps up to the highest
recovered slot and does **not** replace an existing undecided client candidate. A
new leader scans from slot 0 to re-propose decided values at its new ballot. Only a
successful prepare is reused for later slots in that same ballot: failed preparation
does not authorize Phase 2, and a new ballot requires another Phase 1.
Reproposing the same decided value at a higher ballot can advance ballot evidence,
but cannot change the chosen command. The console's `newballot` RPC goes through
`DidaTradeMasterServiceImpl`; a higher ballot updates server state and wakes
`MainLoop`. The existing `activate` handler is only a stub.

`MainLoop` then claims eligible slots and submits Phase 2 attempts to
`PaxosPipeline`'s fixed **four-worker** pool. The pool caps active tasks; new slots
stay within four positions of the next slot to execute, while the scan also revisits
older slots for leader reproposal. `PaxosInstance` holds per-slot proposal ownership,
retry timing, accepted state and decisions; `PaxosLog` is the source of truth for slot
assignment. Normal local assignment avoids reusing a pending ID across slots when
possible, but mandatory Phase 1 recovery can place the same ID in more than one slot.
Workers can finish slots out of order, but never call `TradeManager`.

Decision progress and execution progress are different. For example, slot 0 may be
decided at ballot 1, slot 1 undecided, and slot 2 decided at ballot 4.
`DidaTradeServerState.findMaxDecidedBallot()` scans **all decided log entries** and
returns 4, even across that hole. That replica's `OrderedPaxosExecutor` cannot execute
slot 2 until slot 1 is decided and executable. It advances only through the contiguous
decided prefix, skipping decided NOOPs without a trade and waiting for a decided
client ID's local payload if necessary. A processed ID in a later slot does not change
the trading state twice; this is not a guarantee of full client-RPC idempotency.
The log's internal NOOP value is `-2`; `-1` means no proposal candidate and is not
an executable command.

## Component responsibilities

| Component | Responsibility | Source |
| --- | --- | --- |
| Client and reply collector | Create request IDs, broadcast requests and wait for replies | [`DidaTradeApp`](../src/app/src/main/java/didatrade/app/DidaTradeApp.java), [`GenericResponseCollector`](../src/util/src/main/java/didatrade/util/GenericResponseCollector.java) |
| Client RPC service | Stores local command records, wakes coordination, replies after execution | [`DidaTradeMainServiceImpl`](../src/server/src/main/java/didatrade/server/DidaTradeMainServiceImpl.java) |
| Request history/record | Pending and processed payloads; execution-result wait/notification | [`RequestHistory`](../src/server/src/main/java/didatrade/server/RequestHistory.java), [`RequestRecord`](../src/server/src/main/java/didatrade/server/RequestRecord.java) |
| Server state and configuration | Ballots, leader eligibility, shared services and acceptor transitions | [`DidaTradeServerState`](../src/server/src/main/java/didatrade/server/DidaTradeServerState.java), [`ConfigurationScheduler`](../src/configs/src/main/java/didatrade/configs/ConfigurationScheduler.java) |
| Coordinator | Requests prepare, claims/schedules slots and invokes ordered draining | [`MainLoop`](../src/server/src/main/java/didatrade/server/MainLoop.java) |
| Proposer | Ballot-wide Phase 1, per-slot Phase 2 and local decision on accepted quorum | [`PaxosProposer`](../src/server/src/main/java/didatrade/server/PaxosProposer.java) |
| Pipeline | Fixed four-worker pool, capacity permits and completion signal | [`PaxosPipeline`](../src/server/src/main/java/didatrade/server/PaxosPipeline.java) |
| Log and instance | Slot-to-ID mapping, recovery/NOOP candidates, accepted/learned state and local claims | [`PaxosLog`](../src/server/src/main/java/didatrade/server/PaxosLog.java), [`PaxosInstance`](../src/server/src/main/java/didatrade/server/PaxosInstance.java) |
| Paxos RPC service and processors | Acceptor/learner RPCs; distinct-acceptor Phase 1/2 quorum interpretation | [`DidaTradePaxosServiceImpl`](../src/server/src/main/java/didatrade/server/DidaTradePaxosServiceImpl.java), [`PhaseOneResponseProcessor`](../src/util/src/main/java/didatrade/util/PhaseOneResponseProcessor.java), [`PhaseTwoResponseProcessor`](../src/util/src/main/java/didatrade/util/PhaseTwoResponseProcessor.java) |
| Ordered executor and state machine | Execute decided slots in order and publish RPC results | [`OrderedPaxosExecutor`](../src/server/src/main/java/didatrade/server/OrderedPaxosExecutor.java), [`TradeManager`](../src/core/src/main/java/didatrade/core/TradeManager.java) |
| Console and debug control | `newballot`/`setdebug` RPCs, freeze/slow/crash behavior | [`Console`](../src/console/src/main/java/didatrade/console/Console.java), [`DidaTradeMasterServiceImpl`](../src/server/src/main/java/didatrade/server/DidaTradeMasterServiceImpl.java), [`DebugState`](../src/server/src/main/java/didatrade/server/DebugState.java), [`DebugExitInterceptor`](../src/server/src/main/java/didatrade/server/DebugExitInterceptor.java) |

