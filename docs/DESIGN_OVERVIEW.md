# Design overview: Steps 0–2

This is a guide to the **current Schedule A implementation**. Step numbers follow
[`project-description.pdf`](../project-description.pdf); they are not names for separate
runtime modes. [`request-flow.md`](request-flow.md) describes an earlier baseline and
should not be used as a description of the current proposer or execution loop.

## Overview

DidaTrade replicates a trading state machine across servers. Clients submit user, buy,
sell, balance and dump requests through gRPC; replicas use Paxos log instances to agree
on an execution order. A log value is normally an integer **request/command ID**, not
the full command. Each replica obtains the payload from its local `RequestHistory` and applies
decided commands to its own `TradeManager`. In Schedule A, three servers are learners
and acceptors, while the leader rotates with the ballot; a majority of two acceptors
is the quorum.

## Step 0 — Provided baseline

The provided skeleton already had the essential path: the client broadcast the same
request and ID to the replicas; each server stored a `RequestRecord` in its pending
history; a leader proposed the ID for a numbered Paxos instance; learners recorded a
decision; and the local state machine executed the corresponding command before its
RPC handler replied. `PaxosLog` supplied the ordering, while `RequestHistory` supplied
the command payload and the waiting RPC handler. The original `MainLoop` handled one
slot's consensus and execution before advancing to the next.

This was an **incomplete** Paxos baseline, not the implementation below: its Phase 1
used `PhaseOneBogusProcessor` and could proceed on one reply instead of collecting a
proper quorum. The current code no longer uses that processor or that sequential
`MainLoop` structure.

## Step 1 — Completing Paxos

The console's `setdebug` RPC changes `DebugState`: mode 1 requests a crash, 2
freezes client/Paxos request handling until mode 3 unfreezes it, and 4 enables random
processing delay until mode 5 disables it; mode 0 clears the modes. Freeze controls
request-processing entry points, not every background thread. The console RPC remains
usable to unfreeze a server. `DebugExitInterceptor` defers a pending crash to the
call-completion callback rather than terminating inside the debug RPC handler.

Phase 1 was corrected to require promises from **distinct configured acceptors**.
Accepted history is selected by the highest previously accepted ballot for each
instance; if there is no such value, the leader may use its local candidate. A higher
ballot rejection aborts the stale attempt and advances the server's ballot. Phase 2
likewise succeeds only on a quorum of distinct accepted replies—not merely because
RPCs have finished. The proposer then marks its local slot decided; other replicas
can decide it from Learn notifications. Acceptor promise/accept transitions are
synchronized with the server ballot. The Phase 1 handler serializes collection of
accepted-entry references and its promise against Phase 2, then reads each entry's
accepted value/ballot together; it does not freeze the entire reply as one immutable
log snapshot. An acceptor rejects a different value at the same ballot or a
conflicting value for an already decided slot.
Learners validate the sender and count distinct acceptors **per ballot**, retaining
older-ballot evidence when Learn messages arrive out of order.

In Step 1, each slot still ran its own corrected Phase 1. These safety rules remain
in the current code; Step 2 changes *when Phase 1 is run*, not the requirement to
obtain a safe value from it.

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

This section describes the implemented Multi-Paxos path, **not** Fast Paxos, causal
balance reads, or dynamic membership/state transfer from later assignment steps.

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

## Current request flow

```text
Client broadcasts request + ID to every replica
    -> DidaTradeMainServiceImpl -> RequestHistory.pending; RPC handlers wait
    -> leader MainLoop: successful global Phase 1 for this ballot, then claim slots
    -> PaxosPipeline workers / PaxosProposer: Phase 2(slot, ballot, request ID)
    -> acceptors accept the slot value
         +-> accepted quorum -> proposer marks its local slot decided
         +-> Learn notifications -> learners decide on distinct-acceptor quorum
    -> PaxosLog/PaxosInstance holds each replica's slot decisions
    -> each replica's OrderedPaxosExecutor (in slot order)
    -> RequestHistory[ID] -> TradeManager -> RequestRecord.setResponse()
    -> server RPC reply -> client collector
```

The client currently waits for one reply. The server sends that reply only after **its
own** ordered executor has applied the command, not merely when Paxos decides its ID.

## Why the responsibilities are split

`MainLoop` is the single slot coordinator, so local slot assignment is not spread
across workers. `PaxosProposer` owns the ballot's prepare and each claimed Phase 2
attempt; `PaxosPipeline` owns only bounded worker capacity. `PaxosLog` and
`PaxosInstance` retain proposal and decision state rather than duplicating it in the
pool. Finally, `OrderedPaxosExecutor` is each replica's sole state-machine execution
path, so out-of-order consensus cannot reorder trades or release an RPC before execution.
