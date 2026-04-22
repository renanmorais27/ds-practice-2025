---
title: "feat: 2PC distributed commitment protocol for order execution"
type: feat
status: completed
date: 2026-04-22
---

# 2PC Distributed Commitment Protocol for Order Execution

## Overview

Make the executor service a **2PC coordinator** that drives a Prepare/Commit/Abort protocol across two participants — a new **payment service** and the existing **books_database** — so that the "real" per-order operations (dummy payment execution and stock decrement) happen atomically inside the commit phase.

This replaces the current eager stock-reservation path (orchestrator → `TryDecrement`) with a proper distributed transaction coordinated by the elected executor leader.

## Problem Statement / Motivation

The current pipeline:

1. Orchestrator validates → calls `books_db.TryDecrement` to reserve stock → enqueues the order.
2. Executor leader dequeues → `_execute_order` only logs "confirmed" (stock was already mutated).

This has two problems for the Checkpoint 3 learning objectives:

- **No distributed commitment.** Stock is mutated in a single RPC by the orchestrator; there is no agreement protocol across services.
- **No payment step.** We have no component that represents the "other" side of a transaction, so we cannot demonstrate atomicity across independent participants.

The course task requires: a new dummy payment service, a commitment protocol coordinated by the executor, and the guarantee that participants **execute their real side-effects only after the commit message arrives**.

## Proposed Solution

Implement standard **Two-Phase Commit (2PC)** with the executor leader as coordinator and two participants:

- **Payment service** (new) — dummy "charge the customer" side-effect at commit time.
- **Books database** — stock decrement staged at Prepare, applied at Commit (and replicated to backups only on Commit).

The orchestrator stops mutating stock eagerly; authoritative stock enforcement moves into the Prepare vote of books_db. Because the `/checkout` response is sent *before* the order is executed, response semantics change from *"approved"* to *"accepted for execution"* — the client learns the final outcome through a separate status endpoint that the executor updates after 2PC completes. See [Client notification after async execution](#client-notification-after-async-execution) below.

### Why 2PC (not 3PC)?

- **Simplicity** — textbook algorithm, two round-trips, easy to reason about in a report.
- **Blocking** — 2PC can block participants if the coordinator crashes between Prepare and Commit. 3PC eliminates indefinite blocking with an extra PreCommit phase but assumes synchronous network and costs 3N messages instead of 2N. For a playground prototype with only 2 participants, the extra complexity buys little. We will address the blocking window through (a) the existing bully-elected executor replicas acting as a standby coordinator, and (b) participant-side journaling (bonus). This is the standard pragmatic answer and is defensible in the report.

### Message count (for the report)

With N participants, per transaction:

- **Prepare phase**: N requests + N votes = 2N messages.
- **Commit/Abort phase**: N requests + N acks = 2N messages.
- **Total**: 4N messages (or 4 RPCs with request/response pairs, i.e. 2N RPC round-trips).

For N=2 that's 8 messages per order. 3PC would be 12.

## Technical Approach

### Architecture

```
OrderQueue
    │  Dequeue
    ▼
Executor leader  ─────── Prepare(order_id, …) ─────►  PaymentService
     (coordinator)  ◄───── ready=true/false ───────
                     ─────── Prepare(order_id, items) ─────►  BooksDatabase (primary)
                     ◄───── ready=true/false ─────────────

                      if all ready:
                        Commit(order_id)  →  payment executes dummy charge
                        Commit(order_id)  →  books_db applies stock decrement
                                             (and replicates to backups)
                      else:
                        Abort(order_id)   →  participants drop staged state
```

Participant staging state is keyed by `order_id` (the transaction id). Commit/Abort on an unknown `order_id` is an idempotent no-op — this is important for retries and for recovery after a participant restart.

### Client notification after async execution

Moving stock enforcement off the synchronous `/checkout` path creates a UX gap: the client's POST returns before the 2PC runs, so the user can no longer see the final outcome in the HTTP response. Bridge it with a small status-lookup loop:

```
frontend  ── POST /checkout ─────────────────► orchestrator
                                                ├─ enqueue(order_id)
                                                ├─ order_statuses[order_id] = {"state": "pending"}
                                                └─ return 200 {order_id, status: "accepted"}

frontend ◄─ { "state": "pending" } ────── GET /order-status/<order_id>   (poll every ~1.5s)

executor  ── POST /internal/order-outcome ────► orchestrator
                                                └─ order_statuses[order_id] = {"state": "committed", ...}

frontend ◄─ { "state": "committed" } ────── GET /order-status/<order_id>
             → render "Order Approved" banner
```

Three terminal states: `committed` (success), `aborted` (with `reason`), and `pending` (still executing). The frontend renders a spinner until a non-pending state arrives, then shows the final result. A sensible poll cap of ~30 seconds and a fallback "your order is still being processed — check back later" message covers the case where the executor crashed before reporting.

This is intentionally an in-memory, single-orchestrator design — it matches the course's prototype scope and sidesteps a second distributed-state problem. The mechanism is independent of 2PC and is only used to surface its outcome.

### Protocol definitions

Each participant gets its own `Prepare/Commit/Abort` RPCs in its own proto — we do **not** introduce a shared cross-service proto, to stay consistent with the one-proto-per-service convention already in `utils/pb/`.

#### `utils/pb/payment/payment.proto` (new)

```protobuf
syntax = "proto3";
package payment;

service PaymentService {
  rpc Prepare (PreparePaymentRequest) returns (PrepareResponse);
  rpc Commit  (CommitRequest)         returns (CommitResponse);
  rpc Abort   (AbortRequest)          returns (AbortResponse);
}

message PreparePaymentRequest {
  string order_id = 1;
  int32  amount   = 2;   // dummy, cents; not actually validated
}
message PrepareResponse { bool ready = 1; string reason = 2; }

message CommitRequest   { string order_id = 1; }
message CommitResponse  { bool success = 1; }

message AbortRequest    { string order_id = 1; }
message AbortResponse   { bool aborted = 1; }
```

#### `utils/pb/books_database/books_database.proto` (extend)

Remove `TryDecrement` (its role is subsumed by the 2PC Prepare vote — leaving it exposed would let callers bypass the protocol and desync the `_prepared` accounting). Keep `Read`, `Write`, `Increment`: `Read` is still used for optional orchestrator availability checks; `Write`/`Increment` are used by the primary→backup replication path. Add the 2PC RPCs:

```protobuf
service BooksDatabase {
  // kept:
  rpc Read      (ReadRequest)      returns (ReadResponse);
  rpc Write     (WriteRequest)     returns (WriteResponse);
  rpc Increment (IncrementRequest) returns (IncrementResponse);

  // new — 2PC:
  rpc Prepare (PrepareStockRequest) returns (PrepareStockResponse);
  rpc Commit  (CommitRequest)       returns (CommitResponse);
  rpc Abort   (AbortRequest)        returns (AbortResponse);
}

message StockItem { string title = 1; int32 quantity = 2; }

message PrepareStockRequest {
  string   order_id = 1;
  repeated StockItem items = 2;
}
message PrepareStockResponse { bool ready = 1; string reason = 2; }

message CommitRequest  { string order_id = 1; }
message CommitResponse { bool success = 1; }
message AbortRequest   { string order_id = 1; }
message AbortResponse  { bool aborted = 1; }
```

`CommitRequest`/`AbortRequest` are duplicated across `payment` and `books_database` packages — that's fine, distinct proto packages, existing Python code already aliases `_pb2` modules per service.

### File-by-file changes

#### `utils/pb/payment/payment.proto` — new
Defines the payment service and its Prepare/Commit/Abort RPCs as above.

#### `payment/requirements.txt` — new
```
grpcio==1.78.0
grpcio-tools==1.73.1
protobuf==6.31.1
watchdog==6.0.0
```

#### `payment/Dockerfile` — new
Mirror `books_database/Dockerfile`; compile the payment proto then hot-reload `payment/src/app.py`. Serves on port 50058.

#### `payment/src/app.py` — new

```python
# pseudo-outline
class PaymentServicer(payment_pb2_grpc.PaymentServiceServicer):
    def __init__(self):
        self._lock = threading.Lock()
        self._prepared = {}  # order_id -> amount (tentative)

    def Prepare(self, request, context):
        # Dummy validation — always vote YES for the prototype.
        # (Could randomly return ready=False for testing aborts.)
        with self._lock:
            self._prepared[request.order_id] = request.amount
        logging.info("[payment] Prepared order %s for $%d", request.order_id, request.amount)
        return payment_pb2.PrepareResponse(ready=True)

    def Commit(self, request, context):
        with self._lock:
            amount = self._prepared.pop(request.order_id, None)
        if amount is None:
            # idempotent: either already committed or never prepared
            logging.info("[payment] Commit for unknown order %s (idempotent no-op)", request.order_id)
            return payment_pb2.CommitResponse(success=True)
        logging.info("[payment] EXECUTED payment for order %s: $%d", request.order_id, amount)
        return payment_pb2.CommitResponse(success=True)

    def Abort(self, request, context):
        with self._lock:
            self._prepared.pop(request.order_id, None)
        logging.info("[payment] Aborted order %s", request.order_id)
        return payment_pb2.AbortResponse(aborted=True)
```

#### `books_database/src/app.py` — extend
Add 2PC handlers to `BooksDatabaseServicer`. Three-state transaction bookkeeping (`STAGED`/`COMMITTED`/`ABORTED`) per `order_id` — this is what makes Prepare and Commit idempotent across crashes and retries.

```python
# pseudo-outline (new fields and methods)
STAGED, COMMITTED, ABORTED = "staged", "committed", "aborted"

# order_id -> (state, [(title, qty), ...])
# committed/aborted entries are tombstones — they stay around to make Commit/Abort idempotent.
self._tx = {}

def Prepare(self, request, context):
    # Validate inputs at the boundary.
    for item in request.items:
        if item.quantity <= 0:
            return db_pb2.PrepareStockResponse(ready=False, reason=f"invalid quantity for '{item.title}'")
    with self._lock:
        prior = self._tx.get(request.order_id)
        if prior:
            # Idempotent: re-Prepare on a known order returns the existing verdict.
            state, _ = prior
            return db_pb2.PrepareStockResponse(ready=(state == STAGED))
        # Simple feasibility: current stock must cover this order. For the prototype we
        # assume one in-flight transaction at a time; concurrent-prepare accounting is
        # not needed for the checkpoint demo.
        for item in request.items:
            if self.store.get(item.title, 0) < item.quantity:
                self._tx[request.order_id] = (ABORTED, [])  # tombstone: definitely no
                return db_pb2.PrepareStockResponse(ready=False, reason=f"insufficient '{item.title}'")
        self._tx[request.order_id] = (STAGED, [(i.title, i.quantity) for i in request.items])
        # Bonus: journal_write(self._tx[request.order_id])  — before returning ready=true
    return db_pb2.PrepareStockResponse(ready=True)

def Commit(self, request, context):
    with self._lock:
        entry = self._tx.get(request.order_id)
        if entry is None or entry[0] != STAGED:
            # Unknown or already finalized — idempotent no-op.
            return db_pb2.CommitResponse(success=True)
        _, staged = entry
        for title, qty in staged:
            self.store[title] = self.store.get(title, 0) - qty
        self._tx[request.order_id] = (COMMITTED, staged)
        replicate_payload = list(staged)  # snapshot values for replication outside the lock
        new_values = {t: self.store[t] for t, _ in staged}
    # IMPORTANT: replicate OUTSIDE the lock so a flaky backup can't stall Reads.
    if isinstance(self, PrimaryReplica):
        for title in new_values:
            self._replicate("Write", db_pb2.WriteRequest(title=title, new_stock=new_values[title]))
    return db_pb2.CommitResponse(success=True)

def Abort(self, request, context):
    with self._lock:
        entry = self._tx.get(request.order_id)
        if entry and entry[0] == STAGED:
            self._tx[request.order_id] = (ABORTED, [])
        elif entry is None:
            self._tx[request.order_id] = (ABORTED, [])  # tombstone for late Prepare retries
    return db_pb2.AbortResponse(aborted=True)
```

**Replication honesty** — call this out in the report. Commit replicates per-title via the existing `Write` path *after* releasing `_lock`. Two known gaps for a prototype:
1. A multi-title commit that crashes mid-replication leaves divergent backups with no automatic reconciliation.
2. Because we ship absolute values (`Write(title, new_stock)`) rather than deltas, any concurrent committed order on the same title could produce an out-of-order backup application if replication is not serialized through the primary's decrement lock. Since we replicate *outside* `_lock`, we accept this as a known playground limitation — acceptable because we only have one executor leader driving 2PC at a time.

#### `executor/src/app.py` — main change

Replace `_execute_order` with a 2PC coordinator:

```python
# pseudo-outline
PAYMENT_ADDR = os.environ.get("PAYMENT_ADDR", "payment:50058")
BOOKS_DB_ADDR = os.environ.get("BOOKS_DB_ADDR", "books_db_1:50060")

def _execute_order(self, order_id, items):
    # Build participant stubs and their Prepare requests
    with grpc.insecure_channel(PAYMENT_ADDR) as pay_ch, \
         grpc.insecure_channel(BOOKS_DB_ADDR) as db_ch:
        pay = payment_pb2_grpc.PaymentServiceStub(pay_ch)
        db  = db_grpc.BooksDatabaseStub(db_ch)

        prepares = [
            ("payment", lambda: pay.Prepare(payment_pb2.PreparePaymentRequest(
                order_id=order_id, amount=_dummy_amount(items)), timeout=5)),
            ("books_db", lambda: db.Prepare(db_pb2.PrepareStockRequest(
                order_id=order_id, items=[db_pb2.StockItem(title=i.title, quantity=i.quantity) for i in items]),
                timeout=5)),
        ]

        votes = {}
        for name, call in prepares:
            try:
                resp = call()
                votes[name] = bool(resp.ready)
            except Exception as e:
                logging.warning("[executor] Prepare to %s failed: %s", name, e)
                votes[name] = False

        if all(votes.values()):
            # Phase 2: Commit (bounded retry — covered in lecture).
            self._broadcast(pay, db, order_id, "Commit")
            logging.info("[executor] Order %s COMMITTED", order_id)
            self._notify_orchestrator(order_id, "committed", "")
            return True
        else:
            reason = next((f"{n}: {v}" for n, v in votes.items() if v is False), "vote failed")
            self._broadcast(pay, db, order_id, "Abort")
            logging.info("[executor] Order %s ABORTED (votes=%s)", order_id, votes)
            self._notify_orchestrator(order_id, "aborted", reason)
            return False
```

`_broadcast` retries each participant call on transient failure with a bounded retry count (e.g., 3× with short backoff). This was covered in lecture as the standard coordinator behavior; it mitigates network blips without resolving the coordinator-crash-mid-decision window (that's the 2PC blocking problem, discussed in the bonus analysis).

`_notify_orchestrator` is a best-effort HTTP POST to `http://orchestrator:5000/internal/order-outcome` with `{order_id, outcome, reason}`. If the orchestrator is down, the executor logs and moves on — the frontend will stay on "pending" until the orchestrator is reachable, which is acceptable for a playground.

#### `orchestrator/src/app.py` — simplify + status plumbing
Remove the eager `TryDecrement` / `release_reserved_stock` / `stock_reserved` block (around lines 335–382). Also remove the `db_stub.TryDecrement(...)` call — that RPC is being retired from the proto. Authoritative enforcement moves to the DB's Prepare vote.

Because the `/checkout` response is sent *before* the order has actually been committed by the executor, add:

1. An in-memory status map `order_statuses: dict[str, dict]` keyed by `order_id`. Entries: `{"state": "pending" | "committed" | "aborted", "reason": str}`.
2. The `/checkout` handler seeds `order_statuses[order_id] = {"state": "pending", "reason": ""}` right after enqueue, and its response includes `order_id` + `status: "accepted"` (not "approved") so the frontend knows to poll.
3. A new HTTP endpoint **`POST /internal/order-outcome`** that the executor calls after 2PC completion: body `{order_id, outcome, reason}`. Writes the map.
4. A new HTTP endpoint **`GET /order-status/<order_id>`** that the frontend polls until the state is not `pending`.

Keep everything in-memory; no persistence needed for the prototype. The orchestrator is a single instance, so no coordination across orchestrators is required.

#### `docker-compose.yaml` — add service, wire envs

```yaml
  payment:
    build:
      context: ./
      dockerfile: ./payment/Dockerfile
    ports:
      - 50058:50058
    environment:
      - PYTHONUNBUFFERED=TRUE
      - PYTHONFILE=/app/payment/src/app.py
    volumes:
      - ./utils:/app/utils
      - ./payment/src:/app/payment/src
```

Add to each `executor_N`:
```yaml
      - PAYMENT_ADDR=payment:50058
      # BOOKS_DB_ADDR already present
```

#### `executor/Dockerfile` — compile payment proto too
Add a `grpc_tools.protoc` invocation for `utils/pb/payment` alongside the existing three.

#### `frontend/src/` — polling for async outcome
After a successful POST to `/checkout` that returns `{order_id, status: "accepted"}`, show a "Processing order…" state and start polling `GET /order-status/<order_id>` every ~1500 ms. Render the final banner when the state transitions to `committed` (approved) or `aborted` (with `reason`). Cap polling at ~30s and fall back to a "still processing, check back later" message. This is ~20 lines of JS in the existing checkout handler; no framework changes.

#### Other Dockerfiles
Same `grpc_tools.protoc` command for payment proto in `books_database/Dockerfile` and `payment/Dockerfile` (for its own proto) — copy the existing pattern.

#### `README.md` — update to reflect the new service and protocol

The README is the front door for the course submission and must show the new architecture. Four edits:

1. **Architecture mermaid + paragraph underneath** (lines ~38–51): add `Payment` and `BooksDatabase` nodes, draw 2PC edges from the executor, and rewrite the paragraph to describe 2PC ("leader acts as coordinator, Prepare/Commit/Abort across payment and books_database, side-effects only on Commit") and the async client-notification loop.
2. **Services table** (line ~55): add rows for `Payment (50058)` and `Books Database ×3 (50060)`.
3. **Checkout Flow → Stage 3** (line ~86): rewrite to describe (a) the 2PC round-trip between executor and participants and (b) that `/checkout` returns `accepted` + `order_id` and the frontend polls `/order-status/<id>` until committed/aborted.
4. **New section "Distributed Commitment (2PC)"** after "Leader Election": role assignment, a short mermaid sequence diagram of Prepare/Commit, the message-count and 2PC-vs-3PC trade-off table (from this plan), and a one-paragraph summary of participant recovery + coordinator-failure analysis (with a link to the bonus analysis if we write it as a separate file).

Keep existing prose voice (short clause-level bullets, mermaid for visuals). Do not touch validation / fraud rule tables or the election section.

## System-Wide Impact

- **Interaction graph**: Order dequeue → Executor leader runs 2PC → Payment.Prepare + BooksDb.Prepare → collect votes → Commit or Abort → participants run side-effects on Commit. BooksDb's Commit then triggers the existing primary→backup replication. Orchestrator no longer touches stock mutation at checkout.
- **Error propagation**: gRPC `Exception` on any Prepare counts as a `NO` vote → Abort. Exception on Commit after a decision → retry loop (bounded) → if still failing, log and move on; the participant's idempotent handling + journal replay (bonus) makes eventual retry safe. Aborts never block the queue.
- **State lifecycle risks**: Prepared-but-never-finalized entries are the classic 2PC risk. Mitigations: (a) participants time out stale prepared entries and unilaterally abort (loses safety if coordinator decided Commit — opt-in only for demo), (b) coordinator crash recovery analysis in bonus §2.
- **API surface parity**: The agent-facing (gRPC) API gains 3 new verbs per 2PC participant; these are coordinator-only and not exposed to the orchestrator or frontend.
- **Integration test scenarios**:
  1. Happy path: place an order; assert stock decrements, payment logs "EXECUTED", queue drains.
  2. Insufficient stock: order exceeds stock; assert BooksDb votes NO, abort broadcast, payment logs "Aborted", stock unchanged.
  3. Payment votes NO (toggle via env flag or random seed); assert books_db stays at original stock.
  4. Participant crash mid-flight (kill container between Prepare and Commit); on restart and retry, Commit is idempotent, decrement applies exactly once.
  5. Coordinator crash mid-flight (kill executor leader between Prepare and Commit); the bully election promotes a new leader — discuss in analysis what the new leader knows and what it cannot recover without a decision log.

## Acceptance Criteria

### Functional
- [x] New `payment` service starts via `docker-compose up` and listens on 50058.
- [x] `payment.proto` exposes `Prepare/Commit/Abort`; `books_database.proto` adds the same three RPCs and drops `TryDecrement`.
- [x] Executor leader, upon dequeuing an order, runs 2PC: calls `Prepare` on both participants, then `Commit` on both if all voted ready, else `Abort` on both. Commit/Abort broadcast uses bounded retry on transient failures.
- [x] On Commit, payment logs `"EXECUTED payment for order <id>"` and books_database mutates stock (and replicates to backups).
- [x] On Abort, no side-effects are observable: no payment log, no stock change.
- [x] Orchestrator's `/checkout` returns `{order_id, status: "accepted"}` before 2PC runs; no stock mutation happens on this path.
- [x] Orchestrator exposes `GET /order-status/<order_id>` returning `pending | committed | aborted` and `POST /internal/order-outcome` for executor callbacks.
- [x] Frontend polls `/order-status/<order_id>` after submission and shows the final outcome once the executor reports back.
- [x] `Prepare`, `Commit`, and `Abort` are all idempotent on repeated or out-of-order messages for the same `order_id` (re-Prepare returns prior vote; Commit/Abort on finalized tx is a no-op).
- [x] Prepare rejects negative or zero quantities.

### Bonus — Participant recovery
- [x] Books_database journals the `_tx` map to a JSON file (e.g. `/tmp/books_db_tx.json`) **before** returning a Prepare vote, and updates it on Commit/Abort state transitions. On startup, reload the file into `_tx`.
- [x] Journal entries keep the three-state marker (`staged | committed | aborted`), so a Commit retry after crash never double-applies and a late Prepare retry after Abort returns the stored vote.
- [ ] Test: kill `books_db_1` between Prepare and Commit; restart; re-send Commit; assert stock updates **exactly once**. _(Journaling code in place; crash-injection test not yet run.)_
- [ ] Test: kill `books_db_1` between Commit and its ack; restart; re-send Commit; assert stock stays at the committed value (no second decrement). _(Journaling code in place; crash-injection test not yet run.)_

### Bonus — Coordinator failure analysis (no code required)
- [x] Write a short analysis (~½ page, in the PR description or a `docs/analysis/` note) covering:
  - What each participant's state is in each failure window (before Prepare / after Prepare before decision / after decision partially sent).
  - Why the indefinite-blocking window is inherent to 2PC.
  - Proposed solution: use the existing bully-elected executor standby as a recovery coordinator, **with** a replicated decision log (e.g., write-ahead each coordinator decision to disk and to a peer before broadcasting) — and a termination protocol where a new coordinator polls participants' known state to infer or force Abort.
  - Mention 3PC as an alternative and its cost.

### Quality gates
- [x] `docker-compose up` brings the whole stack up cleanly with the new service.
- [x] A happy-path checkout through the frontend still works end-to-end.
- [x] Logs clearly show the four phases (Prepare sent, votes collected, decision, Commit/Abort completed) per order — these will be used as report evidence.
- [x] `README.md` updated: opening blurb, architecture mermaid, services table, Stage 3 description, sequence diagram, new "Distributed Commitment (2PC)" section, project structure, known limitations.

## 2PC Trade-offs — for the course report

| Dimension              | 2PC                               | 3PC                                     |
|------------------------|-----------------------------------|-----------------------------------------|
| Phases                 | 2                                 | 3                                        |
| Messages per tx (N=2)  | 8                                 | 12                                       |
| Blocking on coord fail | Yes, between Prepare and decision | No (with synchrony + no partitions)      |
| Complexity             | Low                               | Medium                                   |
| Assumptions            | Async OK                          | Bounded delays, fail-stop                |

Blocking probability in 2PC is a function of (time between receiving vote and broadcasting decision) × (coordinator failure rate). Shrinking the first factor (decision durability + fast broadcast) is the practical mitigation we adopt, rather than moving to 3PC.

## Implementation Phases

### Phase A — Participants + coordinator (core task)
- Add `utils/pb/payment/payment.proto`, `payment/` service dir, Dockerfile, requirements, compose entry. Verify it starts.
- Extend `books_database.proto` (add 2PC RPCs, remove `TryDecrement`); implement `Prepare/Commit/Abort` in `books_database/src/app.py` with the three-state `_tx` map; update `PrimaryReplica.Commit` to replicate outside the lock.
- Implement `Prepare/Commit/Abort` in `payment/src/app.py`.
- Replace executor's `_execute_order` with the 2PC coordinator; keep bounded retry in `_broadcast` (lecture-standard behavior).
- Remove eager stock-decrement block from orchestrator; add `order_statuses` map + `GET /order-status/<id>` + `POST /internal/order-outcome` + `_notify_orchestrator` call from executor.
- Update frontend to poll the status endpoint after checkout.
- End-to-end test: happy-path commit + insufficient-stock abort + payment-no abort. Capture logs for the report.

### Phase B — Bonuses (optional but scoped)
- JSON journaling of `_tx` in books_database (write on each state transition, reload on boot); the two crash-recovery tests in the bonus criteria.
- Coordinator-failure analysis — short write-up (PR description or `docs/analysis/coordinator-failure.md`) covering crash windows, why 2PC blocks, and the bully-elected standby + replicated decision log proposal. 3PC comparison reuses the trade-offs table.

## Success Metrics

- `docker-compose up` + one checkout through the frontend yields logs from orchestrator, executor leader, payment, and books_db showing the four 2PC events in order.
- Killing `books_db_1` between Prepare and Commit, restarting it, and re-sending Commit leaves stock decremented exactly once.
- Killing the executor leader mid-flight: bully election promotes a new leader; the system does not process further orders until the analysis-proposed recovery is performed (expected blocking; this is the teaching point).

## Dependencies & Risks

- **Risk**: reusing method names `Commit`/`Abort` across payment and books_database protos — fine because they live in distinct proto packages, but generated Python modules must be imported with aliases (`payment_pb2`, `db_pb2`) exactly as other services already do.
- **Risk**: primary-to-backup replication currently replicates `Write/TryDecrement/Increment`. Commit-path replication must call `Write(title, new_stock)` per affected title (or introduce `CommitReplicate`) to keep backups coherent. Choose the simplest path: reuse `Write` during Commit on the primary.
- **Dependency**: no new external libraries — existing `grpcio`/`protobuf` versions are sufficient.

## Sources & References

### Internal references
- [executor/src/app.py:209](executor/src/app.py#L209) — current `_execute_order` that will be replaced.
- [orchestrator/src/app.py:335-382](orchestrator/src/app.py#L335-L382) — eager stock-reservation block to remove.
- [books_database/src/app.py:27-102](books_database/src/app.py#L27-L102) — servicer + primary replica to extend.
- [utils/pb/books_database/books_database.proto](utils/pb/books_database/books_database.proto) — proto to extend.
- [utils/pb/order_queue/order_queue.proto](utils/pb/order_queue/order_queue.proto) — `BookItem` shape used when handing items to the coordinator.
- [docker-compose.yaml](docker-compose.yaml) — add `payment` service and wire `PAYMENT_ADDR` into executor replicas.
- [docs/plans/2026-03-25-feat-order-queue-executor-services-plan.md](docs/plans/2026-03-25-feat-order-queue-executor-services-plan.md) — prior plan that introduced OrderQueue and Executor; conventions for new services mirror it.

### Conceptual references
- Tanenbaum & Van Steen, *Distributed Systems*, ch. on Distributed Commit (2PC/3PC).
- Gray & Reuter, *Transaction Processing: Concepts and Techniques*, §7 Two-Phase Commit — canonical description of the blocking window.
