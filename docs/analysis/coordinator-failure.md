---
title: "2PC coordinator failure analysis"
type: analysis
date: 2026-04-22
---

# 2PC coordinator failure analysis

The executor leader is the coordinator of the Prepare/Commit/Abort protocol driving the
payment service and the books_database primary. This note walks through what happens when
the coordinator crashes at each point in the protocol, why 2PC fundamentally blocks in one
of those windows, and what a production-grade extension would do about it.

## Participant state per failure window

Three windows matter, pinned to the coordinator's progress through the protocol:

| Window | Coordinator state | Payment state | Books DB state | What participants know |
|--------|--------------------|----------------|----------------|-------------------------|
| **(W1)** before sending any Prepare | no record | not contacted | not contacted | nothing has happened — safe to drop |
| **(W2)** after collecting all votes, before the decision is broadcast | knows the votes but hasn't told anyone | `STAGED` (voted YES) or `ABORTED` (voted NO tombstone) | `STAGED` with items, or `ABORTED` tombstone | each knows only its own vote; neither knows the other's; neither knows the decision |
| **(W3)** after sending Commit/Abort to **some** but not all participants | decision made, partially broadcast | `COMMITTED`/`ABORTED` if reached, else `STAGED`/`ABORTED` tombstone | same | at least one participant has heard the decision; a recovery can ask around |

The prototype's three-state `_tx` map in the books database (and equivalent map in the
payment service) is what makes these windows survivable. A `STAGED` entry means "I voted
YES and owe the coordinator a retry-safe Commit or Abort"; a finalized entry (`COMMITTED`
or `ABORTED`) is a tombstone so a re-delivered message is a no-op. The books database's
optional JSON journal (bonus) makes this state survive a crash.

## Why 2PC blocks in W2 — and only in W2

W1 is trivially safe: no side effects, no state. W3 is recoverable: at least one
participant knows the decision, so a new coordinator can poll participants and replay it.
W2 is the hard one — every participant voted YES (or is a tombstone) but nobody knows the
coordinator's decision. A recovery coordinator that asks around learns only "all of us
voted YES"; it cannot distinguish "the old coordinator decided Commit and died before
telling anyone" from "the old coordinator hadn't decided yet." Unilaterally aborting is
unsafe because a committing coordinator may have told a client the order succeeded;
unilaterally committing is unsafe for the same reason in mirror.

This is inherent to 2PC: the protocol's safety property is "participants agree on the
outcome," but in W2 none of the surviving nodes knows what that outcome is. 2PC chooses
**blocking** over unsafety — participants that voted YES stay `STAGED`, holding their
stock reservation, until the real coordinator returns or a human intervenes.

## Proposed extension: standby coordinator + replicated decision log

The executor service already runs three replicas with bully-algorithm leader election.
Turning that into a usable 2PC recovery mechanism requires two additions:

1. **Replicate each coordinator decision before broadcasting it.** Before the leader
   sends the first `Commit` (or `Abort`) message, it writes the decision to its own
   durable log *and* pushes it to at least one peer executor. Only then does it start
   Phase 2. This collapses W2 into a much smaller window: if the leader dies after
   deciding but before replicating, the new leader sees no record of a decision and can
   safely Abort; if the leader dies after replicating, the new leader reads the decision
   from its own replicated log and finishes Phase 2. Participants' idempotent Commit/Abort
   make the replay safe.

2. **Termination protocol on leader change.** When the bully election promotes a new
   leader, it runs a termination step before resuming order dequeue:
   - For every `order_id` in its replicated decision log, re-send Commit/Abort to
     participants. They are idempotent; this catches the W3 recovery case.
   - Ask each participant to report its `STAGED` order ids. For any `STAGED` entry the
     new leader has **no** log record for, it must decide Abort (the old leader hadn't
     durably decided) and broadcast it.
   - Only then does the new leader resume consuming the order queue.

This retains 2PC's two-phase structure and message count in the common path. The cost is
one extra replication round-trip per decision. The blocking window shrinks from "indefinite"
to "the time between the coordinator deciding and its replication partner acking" —
typically milliseconds.

## Why not 3PC?

3PC adds a `PreCommit` phase between Prepare and Commit so that participants that reached
PreCommit can unilaterally commit on coordinator failure, eliminating the indefinite
blocking. Costs: one extra round-trip (12 messages per tx for N=2 instead of 8), and it
assumes bounded network delay and fail-stop — if the network can partition, 3PC can still
get stuck or violate safety. For a course prototype with two participants and a single
LAN-local executor leader, the coordinator-replication approach above buys most of what
3PC buys at lower message cost, and the assumptions are easier to defend.

## Summary

| Window | Safe? | Recovery |
|--------|-------|----------|
| W1 (before Prepare) | Yes | Drop |
| W2 (after votes, before decision) | Unsafe *without* a replicated decision log | Proposed fix: write decision to peer before broadcasting |
| W3 (after partial broadcast) | Yes | New leader polls participants + replays decision |

The prototype implements the idempotent participant side (three-state `_tx` map, optional
journal) and leaves the coordinator-side decision replication as a documented extension.
This matches the checkpoint's learning objective: show that 2PC is correct in the happy
path and in recoverable crash windows, and identify the one window where it cannot
progress without additional mechanisms.
