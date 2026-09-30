# End-to-End Request Flow

This document follows one `buy` request through the baseline implementation,
from the client to Paxos, state-machine execution, and the client reply. It describes
the code as it exists today; it does not describe the later Multi-Paxos or corrected
Phase 1 work.

## 1. Overview

```text
Client / DidaTradeApp.buy
        |
        | broadcasts BuyRequest(reqid, userid, quantity)
        v
+--------------------------- EVERY REPLICA ----------------------------+
| DidaTradeMainServiceImpl.buy                                         |
|        |                                                             |
|        v                                                             |
| RequestHistory.pending -> wake MainLoop; RPC handler waits           |
|        |                                                             |
|        v                                                             |
| MainLoop processes the next sequential Paxos instance                |
+--------+-------------------------------------------------------------+
         |
         | only the configured ballot leader proposes
         v
+----------------------- LEADER / PROPOSER ONLY -----------------------+
| Phase 1: select or carry forward a command ID                        |
| Phase 2: propose (instance, ballot, command ID)                      |
+--------+-------------------------------------------------------------+
         |
         | Phase 1 and Phase 2 RPCs
         v
+------------------------ CONFIGURED ACCEPTORS ------------------------+
| DidaTradePaxosServiceImpl handles prepare/accept requests            |
| Accepted Phase 2 values are sent to every learner                    |
+--------+-------------------------------------------------------------+
         |
         | LearnRequest(instance, ballot, command ID)
         v
+---------------------- EVERY REPLICA / LEARNER -----------------------+
| PaxosLog[instance] = decided command ID                              |
|        |                                                             |
|        v                                                             |
| RequestHistory[command ID] -> DidaTradeCommand                       |
|        |                                                             |
|        v                                                             |
| TradeManager.acquire(...) -> RequestRecord.setResponse(...)          |
|        |                                                             |
|        v                                                             |
| DidaTradeMainServiceImpl -> BuyReply(reqid, result)                  |
+--------+-------------------------------------------------------------+
         |
         v
Client response collector (currently continues after one reply)
```

The role boundaries describe actions, not necessarily different machines. A replica can
simultaneously be the ballot leader, a configured acceptor, and a learner.

The central relationship is:

```text
PaxosLog slot -> request/command ID -> RequestHistory record -> command payload
                                                        |
                                                        v
                                                   TradeManager
```

`PaxosLog` orders integer command IDs. `RequestHistory` supplies the corresponding
`DidaTradeCommand` payload and connects execution back to the waiting RPC handler.

## 2. Detailed `buy` walkthrough

1. **The client creates the request ID and request.** In
   `DidaTradeApp.buy(int uid, int quantity)`, the client increments its local
   `sequence_number` and computes:

   ```text
   reqid = sequence_number * 100 + client_id
   ```

   It puts `reqid`, `uid`, and `quantity` into the `BuyRequest` defined in
   `DidaTradeMain.proto`.

2. **The client broadcasts the payload.** `DidaTradeApp.buy(...)` invokes the
   asynchronous `buy` RPC on every configured `DidaTradeMainService` stub. Every
   replica therefore receives the same request ID and command data. The client thread
   then waits in `GenericResponseCollector.waitForQuorum(responses_needed)`. Despite
   that method name, the current client value of `responses_needed` is `1`, so one RPC
   reply is sufficient for the client to continue.

3. **A replica records the request as pending.** In
   `DidaTradeMainServiceImpl.buy(...)`, the server extracts the three protobuf fields,
   creates `DidaTradeCommand(BUY, uid, quantity)`, wraps it in
   `RequestRecord(reqid, command)`, and calls
   `RequestHistory.addToPending(reqid, requestRecord)`.

4. **The server RPC handler waits.** The handler calls `MainLoop.wakeup()` and then
   blocks in `requestRecord.waitForResponse()`. Receiving the request is therefore not
   enough to send a reply: that replica waits until its state machine executes the
   request and publishes a result on this `RequestRecord`.

5. **`MainLoop` selects the next log slot.** `MainLoop.run()` increments its local
   `next_log_entry` and calls `processEntry(entryNumber)`. It starts at instance `0` and
   processes instances sequentially. `PaxosLog.testAndSetEntry(entryNumber)` obtains or
   creates the `PaxosInstance` for that already selected slot.

   Paxos does not choose a slot for the pending request. `MainLoop` chooses which
   sequential instance it is currently processing, and Paxos chooses the **value** for
   that instance:

   ```text
   log slot / Paxos instance -> command ID
   ```

6. **The leader chooses an initial value.** While the entry is undecided,
   `MainLoop.processEntry(...)` reads a record using
   `RequestHistory.getFirstPending()`. If this server is the configured leader for the
   current ballot, it initially sets `phase_two_value` to that record's request ID.

7. **Phase 1 asks acceptors about the slot.** The leader sends
   `PhaseOneRequest(instance, requestballot)` to the ballot's configured acceptors. In
   `DidaTradePaxosServiceImpl.phaseone(...)`, an acceptor returns `accepted=true` for the
   Phase 1
   request when the requested ballot is at least the server's current ballot. In that
   case, the acceptor advances its current ballot if needed, records the Phase 1 ballot
   in `read_ballot`, and replies with the value and write ballot already stored for the
   instance.

   Phase 1 may therefore replace the leader's initially chosen pending request ID with a
   previously accepted request ID. In the current baseline, `MainLoop` uses
   `PhaseOneBogusProcessor`; the exact limitation is recorded in
   [Current baseline notes](#6-current-baseline-notes).

8. **Phase 2 proposes the selected ID.** If Phase 1 is not treated as aborted, the
   leader sends a `PhaseTwoRequest` containing:

   - `instance`: the current `entry_number`;
   - `requestballot`: the current ballot;
   - `value`: the selected request/command ID.

   `DidaTradePaxosServiceImpl.phasetwo(...)` accepts the request when its ballot is at
   least the acceptor's current ballot. On acceptance it writes the ID to
   `PaxosInstance.command_id`, writes the ballot to `PaxosInstance.write_ballot`, and
   advances the server's current ballot when necessary. A lower-ballot request is
   rejected with the server's higher ballot in the reply.

9. **The proposer evaluates Phase 2.** `PhaseTwoResponseProcessor` ends successfully
   after the configured number of accepted replies. It ends unsuccessfully as soon as
   it sees a rejection, retaining the highest reported ballot. On proposer-side success,
   `MainLoop` records the completed ballot and directly sets its local entry's
   `command_id` and `decided` fields.

10. **Acceptors notify learners.** After accepting Phase 2,
    `DidaTradePaxosServiceImpl.phasetwo(...)` asynchronously sends
    `LearnRequest(instance, ballot, value)` to every configured learner. In
    `DidaTradePaxosServiceImpl.learn(...)`, a learner stores the ID and ballot for a new
    accept ballot and counts same-ballot accept notifications in
    `PaxosInstance.n_accepts`. When that count reaches the schedule's quorum, it marks
    the entry `decided`, updates the completed ballot, and wakes `MainLoop`.

    These states are distinct:

    - **proposed**: a leader has selected an ID and sent it in Phase 2;
    - **accepted**: an acceptor has stored the Phase 2 ID and write ballot;
    - **learned/decided**: a learner has counted a quorum of acceptance notifications,
      or the successful proposer has marked its local entry decided directly;
    - **executed**: `MainLoop` has applied the decided command to `TradeManager`.

11. **The replica resolves the decided ID to a command.** After an entry becomes
    decided, `MainLoop` calls
    `RequestHistory.getIfPending(nextEntry.command_id)`. The Paxos entry contains only
    the integer ID, so the locally received `RequestRecord` is required to recover the
    `DidaTradeCommand` payload.

    Decision and client-request delivery are independent. If the decision reaches a
    replica before its copy of the client request, `MainLoop` waits on its own monitor
    until `DidaTradeMainServiceImpl` adds that record and calls `MainLoop.wakeup()`.

12. **The state machine executes BUY.** `MainLoop` reads the command from the record.
    For action `BUY`, it calls
    `TradeManager.acquire(command.getUserId(), command.getQuantity())`.
    `TradeManager.acquire(...)` finds the user, calculates the amount as the requested
    percentage of the user's balance, withdraws that amount, creates a `BuyOrder`, and
    runs the order-matching logic. It returns `false` if the user does not exist or the
    calculated amount is not positive; otherwise it returns `true` after accepting and
    attempting to match the order.

13. **Execution publishes the response.** `MainLoop` calls
    `RequestRecord.setResponse(result)`. This stores the boolean, marks the response as
    available, and calls `notifyAll()`. It then calls
    `RequestHistory.moveToProcessed(reqid)`, removing the record from `pending` and
    retaining it in `processed`.

14. **The server RPC returns.** The notification lets
    `DidaTradeMainServiceImpl.buy(...)` return from `waitForResponse()`. The handler
    creates `BuyReply(reqid, result)`, calls `responseObserver.onNext(reply)`, and then
    calls `responseObserver.onCompleted()`.

15. **The client completes its wait.** `CollectorStreamObserver.onNext(...)` adds the
    reply to the `GenericResponseCollector`, waking the client thread. With the current
    `responses_needed == 1`, `DidaTradeApp.buy(...)` takes the first collected result and
    continues once one replica has replied.

## 3. Component responsibilities

| Component | Source | Responsibility in the request path |
| --- | --- | --- |
| `DidaTradeApp` | [`DidaTradeApp.java`](../src/app/src/main/java/didatrade/app/DidaTradeApp.java) | Generates the request ID, builds one `BuyRequest`, broadcasts it to all configured replicas, and waits for the configured number of replies. |
| `DidaTradeMainServiceImpl` | [`DidaTradeMainServiceImpl.java`](../src/server/src/main/java/didatrade/server/DidaTradeMainServiceImpl.java) | Converts the protobuf request into a local command and record, adds it to `RequestHistory`, wakes `MainLoop`, waits for execution, and returns the protobuf reply. |
| `RequestHistory` | [`RequestHistory.java`](../src/server/src/main/java/didatrade/server/RequestHistory.java) | Maintains the replica-local `pending` and `processed` maps keyed by request ID. It supplies both candidate pending work and the payload for a decided ID. |
| `RequestRecord` | [`RequestRecord.java`](../src/server/src/main/java/didatrade/server/RequestRecord.java) | Holds the request ID and `DidaTradeCommand`. It is also the monitor through which `MainLoop` publishes the execution result to the waiting RPC handler. |
| `MainLoop` | [`MainLoop.java`](../src/server/src/main/java/didatrade/server/MainLoop.java) | Processes sequential log instances, leads Phase 1/Phase 2 when eligible, waits for decisions and missing local payloads, executes decided commands, and publishes results. |
| `PaxosLog` / `PaxosInstance` | [`PaxosLog.java`](../src/server/src/main/java/didatrade/server/PaxosLog.java), [`PaxosInstance.java`](../src/server/src/main/java/didatrade/server/PaxosInstance.java) | Maps integer instance numbers to Paxos state. An instance stores a command ID, read/write/accept ballots, acceptance count, and decision state—not the full command payload. |
| `DidaTradePaxosServiceImpl` | [`DidaTradePaxosServiceImpl.java`](../src/server/src/main/java/didatrade/server/DidaTradePaxosServiceImpl.java) | Implements acceptor handling for Phase 1 and Phase 2, sends accepted values to learners, and marks learned entries decided after enough notifications. |
| `TradeManager` | [`TradeManager.java`](../src/core/src/main/java/didatrade/core/TradeManager.java) | Implements the replicated trading state machine; for BUY it reserves balance, creates a buy order, and invokes matching. |
| `GenericResponseCollector` | [`GenericResponseCollector.java`](../src/util/src/main/java/didatrade/util/GenericResponseCollector.java) | Coordinates asynchronous replies. On the client path it releases `DidaTradeApp` after the required replies; on Paxos paths it delegates reply interpretation to the phase-specific processor. |

`RequestHistory` provides information that `PaxosLog` deliberately does not. The log
answers, "which request ID executes in this position?" The history answers, "what is
the command for that ID, and which RPC handler is waiting for its result?" `MainLoop`
joins those two pieces and applies the command to `TradeManager`.

### `RequestHistory` operations

- `addToPending(id, record)` stores a newly received local request.
- `getFirstPending()` returns one pending record for a leader to propose.
- `getIfPending(id)` resolves a decided command ID to its local record.
- `moveToProcessed(id)` removes the record from `pending` and stores it in
  `processed` after execution.

## 4. Key IDs and concepts

| Name | Meaning |
| --- | --- |
| Client ID | Identifies the client process. The app accepts values from `1` through `99`. |
| Sequence number | A client-local counter incremented once per operation. It starts at `0` for a new `DidaTradeApp`. |
| Request ID / command ID | `sequence_number * 100 + client_id`. The client sends it with the full request; Paxos uses this same integer as its value. |
| Paxos instance / log position | The sequential state-machine position currently processed by `MainLoop` (`0`, `1`, ...). It is not the request ID. |
| Ballot number | Identifies a Paxos/configuration round, controls its leader and acceptors, and orders competing prepare/accept attempts. It is not a log position. |

Because the client broadcasts the same `reqid`, `userid`, and `quantity` to all replicas,
each replica can normally associate the decided integer with the same operation through
its local `RequestHistory`.

## 5. Synchronization and waiting points

- **Client wait:** `DidaTradeApp.buy(...)` waits in
  `GenericResponseCollector.waitForQuorum(...)`. The current threshold is one reply.
- **RPC-handler wait:** each `DidaTradeMainServiceImpl.buy(...)` invocation waits in its
  local `RequestRecord.waitForResponse()` until state-machine execution calls
  `setResponse(...)`.
- **Main-loop work/decision wait:** when the current slot is not decided and the local
  server cannot make progress, `MainLoop.processEntry(...)` waits until a client
  request, learned decision, or ballot-related event calls `MainLoop.wakeup()`.
- **Missing-payload wait:** after learning an ID, `MainLoop` may wait until the same
  replica receives the corresponding client request and adds its `RequestRecord`.
- **Paxos response waits:** the proposer uses `GenericResponseCollector.waitUntilDone()`
  for both phases; the phase-specific processor determines when collection is done.

## 6. Current baseline notes

- `MainLoop` currently uses `PhaseOneBogusProcessor`, not a quorum-based Phase 1
  processor. It completes on the first Phase 1 reply, does not propagate that reply's
  `accepted` flag into its own result, and carries forward only that reply's reported
  value when its value ballot is non-negative.
- The client sends the same complete request to every replica.
- Paxos reaches consensus only on the integer request ID, not the command payload. This
  is why `MainLoop` can know the decision but still have to wait for the command
  locally.
