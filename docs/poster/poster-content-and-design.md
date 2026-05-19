# A0 Poster Content and Design Suggestions

Working title: **Atomic Checkout in a Distributed Bookstore**

Subtitle: **A microservice checkout that tracks causality with vector clocks and commits payment and stock changes with two-phase commit.**

Authors: Andrius Matšenas, Renan Morais, Leopold Pichonneau

Affiliation: Distributed Systems course, University of Tartu

## Poster Thesis

Make the poster about one reader-friendly question:

> What has to happen after a shopper clicks "Submit order" so that independent services agree on the final result?

The strongest story is not "we used many services." It is:

1. The checkout first validates an order through a causally tracked event graph.
2. Approved orders move to a queue so the web request can return quickly.
3. One elected executor coordinates the irreversible side effects.
4. Two-phase commit keeps payment and stock updates aligned, while exposing a known blocking trade-off.

## Evidence Reviewed

- Current `master` at `8b816de`, tagged `checkpoint-3`.
- `README.md`, `docker-compose.yaml`, core service files, protobufs, and `docs/analysis/coordinator-failure.md`.
- Git history across local and remote branches, including `origin/locust` and `origin/Scenario2`.
- Main evolution: template bookstore -> checkpoint 1 polish -> vector-clock event flow -> order queue/executor -> replicated books DB + payment + 2PC -> load-test scenarios.

## Recommended A0 Layout

Use **A0 portrait**. The poster should read as a clear Z-shaped path:

1. **Top left:** title and the one-sentence research question.
2. **Top right:** authors, affiliation, and system model summary.
3. **Middle band:** the main architecture/checkout path explanation.
4. **Lower left:** key insights from the implementation.
5. **Lower right:** challenges, limitations, and collaboration timeline.

For this phase, use **text-based description containers** instead of rough illustrations. Each container should state what the final diagram must communicate. This gives the layout structure now, while leaving room to replace the containers with polished diagrams later.

Keep body text to short blocks of 25-45 words. Use numbered sections and consistent container titles so the reader can follow the Z path without effort.

## Section Content

### Introduction / Background

Recommended poster text:

> Online checkout looks like one click, but the order touches services that can fail independently. We built a bookstore prototype where validation, fraud checks, suggestions, queueing, payment, and stock updates are coordinated without shared memory.

### System Model

Use a compact list:

- HTTP at the edge, gRPC between backend services.
- Dockerized services with independent state and failure points.
- Messages can fail or time out.
- Vector clocks travel in metadata to explain causal order.
- A single elected executor consumes the order queue.

Avoid listing every port in body text. Put ports only in a small diagram annotation if needed.

### System Architecture

For this design phase, use a text-based illustration brief instead of a drawn diagram. The container should describe:

- Frontend -> Orchestrator over HTTP.
- Orchestrator -> Transaction Verification, Fraud Detection, Suggestions over gRPC.
- Verification-passed orders -> Order Queue.
- Executor replicas elect a leader; only the leader dequeues.
- Executor leader runs 2PC with Payment and Books DB primary.
- Books DB primary replicates committed stock values to backups.

Caption idea:

> The orchestrator decides whether an order is valid. The executor leader decides whether the side effects are committed.

### Sequence / Event Flow

For this design phase, use a compact text-flow container:

```text
init
  |-- a: items present ---- c: card format --|
  |                                          |-- e: card fraud -- f: suggestions
  |-- b: user data -------- d: user fraud ---|
```

Short caption:

> Vector clocks reveal which checks happened concurrently and where the branches joined.

### Key Insights

Use four large numbered statements:

1. **Causality is visible.** Vector clocks show real concurrency, not just log order.
2. **Side effects wait.** Payment and stock changes happen only after a commit decision.
3. **One leader acts.** Bully election prevents executor replicas from processing the same queue item.
4. **Retries are safe.** Participants keep per-order states so repeated Commit or Abort messages are harmless.

### Challenges

Keep these honest and concise:

- **2PC can block.** After YES votes but before a decision reaches participants, no survivor can safely decide alone.
- **Async checkout needs feedback.** The frontend receives `accepted`, then polls for `committed` or `aborted`.
- **Replication is a prototype trade-off.** Books DB replication is best-effort after the primary commits.
- **Some state is in memory.** Queue and order-status state are simple for the demo, not crash durable.

### Collaboration

Use a visual timeline rather than commit counts:

- Feb 25: suggestions and transaction-verification services added.
- Mar 2: checkpoint 1 wrap-up, logging, docs, and frontend polish.
- Mar 11-18: order flow refactored into init plus event execution; vector clocks added.
- Mar 25-Apr 6: order queue and replicated executors with bully election.
- Apr 22-May 5: books database, payment participant, 2PC, recovery analysis.
- May 13 branches: Locust load-test scenarios.

Git aliases to normalize before the final poster: `Andrius` and `Andrius Matšenas`; `Renan` and `Renan Morais`; `LeopoldPichonneau`; also clarify whether `KPPHC` is one teammate's alias before using any contribution chart.

## Design Direction

Use a clean technical editorial style: calm, high contrast, and strongly structured. The current concept should be text-led with restrained placeholder containers; final illustrations can replace those containers once the content hierarchy is approved.

Palette, maximum 5 colors:

| Role | Color |
| --- | --- |
| Ink text | `#14213d` |
| Paper background | `#f7f8f3` |
| Teal systems | `#007a7a` |
| Coral commit path | `#d84a3a` |
| Gold insight accent | `#d69e2e` |

Typography:

- Title: 80-96 pt, serif or strong display face.
- Section headings: 28-36 pt, uppercase or small caps.
- Body: 18-24 pt, short lines.
- Diagram labels: 16-22 pt, never below 14 pt for A0 print.

Visual hierarchy:

- Largest element: title plus main architecture description container.
- Second largest: system model and key insights.
- Third largest: 2PC/failure-window description.
- Smallest: implementation details and source notes.

Print considerations:

- Export with background graphics enabled.
- Prefer vector diagrams over screenshots in the final version.
- Keep lines at least 1.5-2 pt.
- Leave 20-30 mm outer margin.
- Test by printing/exporting at A4 first; if text is hard to read at A4, it will still feel crowded at A0.

## What To Cut

Do not include:

- Full validation-rule table.
- Full protobuf names or generated file details.
- Raw vector-clock JSON.
- Every service port.
- Long commit lists.
- AI suggestion prompt details.

These belong in the report or demo, not the poster.

## Evaluation Fit

- **Clarity and visual appeal:** clear Z-shaped reading order, then final diagrams once the text containers are approved.
- **Content relevance:** all sections map directly to the system model, architecture, insights, and challenges.
- **Depth and creativity:** show the 2PC blocking window and proposed replicated decision-log mitigation.
- **Collaboration:** use the git timeline to show how the three-person implementation evolved over checkpoints.

## Accompanying Concept

See `docs/poster/poster-concept.html` for a self-contained A0 portrait HTML mockup with text-based description containers.
