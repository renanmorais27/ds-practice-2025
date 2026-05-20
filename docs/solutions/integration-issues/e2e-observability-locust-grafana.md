---
title: "Building E2E evaluation from Locust and Grafana"
category: integration-issues
date: 2026-05-20
tags:
  - locust
  - grafana
  - opentelemetry
  - e2e-testing
  - distributed-systems
---

# Building E2E Evaluation From Locust And Grafana

## Problem

Grafana and Locust were present, but they did not yet prove the full bookstore checkout flow. Grafana showed partial service metrics, while Locust generated traffic without consistently asserting terminal order outcomes or exporting scenario-level signals that could explain where failures happened.

## Root Cause

The system had two separate views of behavior: Locust saw the client journey, and Grafana saw only the instrumented backend pieces. Missing trace propagation, missing service metrics, and missing deterministic scenarios meant the dashboard could not reliably explain checkout acceptance, queueing, 2PC commit/abort behavior, stock conflicts, or fraudulent denials.

## Solution

Treat Locust as the E2E truth source and Grafana as the explanation layer:

1. Add shared OpenTelemetry setup for all Python services.
2. Propagate trace context through gRPC metadata and the executor outcome callback.
3. Instrument the main service boundaries: checkout, verification, fraud, suggestions, queue, executor election/dequeue, 2PC prepare/finalize, payment, DB prepare/commit/abort, and replication.
4. Refactor Locust into named scenarios that assert final outcomes and emit synthetic `E2E` events.
5. Add a test-only stock fixture for same-book conflict scenarios.
6. Expand Grafana panels so Locust outcomes can be read alongside service timings and traces.

Key shared helper:

```python
def inject_trace_metadata(metadata=()):
    carrier = {}
    propagate.inject(carrier)
    trace_metadata = tuple(
        (key, value) for key, value in carrier.items() if key.lower() in TRACE_CONTEXT_KEYS
    )
    return tuple(metadata) + trace_metadata
```

Key Locust pattern:

```python
terminal = poll_until_terminal(user, order_id, scenario_name, timeout=timeout)
if terminal in expected_terminals:
    fire_e2e_event(user, f"checkout_to_{terminal}", started_at, scenario_name, terminal)
    return

if terminal == "timeout":
    fire_e2e_event(
        user, "checkout_timeout", started_at, scenario_name, "timeout",
        TimeoutError(f"{order_id} did not reach {sorted(expected_terminals)} within {timeout}s"),
    )
```

Key conflict fixture:

```yaml
services:
  books_db_1:
    environment:
      - 'BOOKS_DB_INITIAL_STOCK_JSON={"Distributed systems.": 1, "Introduction to gRPC.": 5000}'
```

## Verification

Static validation covered:

- Python compilation for instrumented services and Locust files.
- Grafana dashboard JSON parsing.
- Docker Compose config rendering for normal E2E and low-stock conflict modes.

Live validation still requires Docker to be running:

- Start with `docker compose --profile e2e up --build`.
- Open Grafana at `http://localhost:3000` and Locust at `http://localhost:8089`.
- Run `single_non_fraudulent_order`, `mixed_orders_simultaneous`, `payment_abort`, and `same_book_conflict_low_stock`.
- Confirm Locust E2E outcomes, queue wait, executor/2PC votes, payment operations, DB stock, and Tempo traces populate for the same run.

## Prevention Tip

Do not count a Grafana dashboard as E2E test coverage by itself. For distributed flows, require every scenario to have both a client-side assertion in Locust and a backend explanation path in traces/metrics, correlated by scenario, run id, and order id.

## Related Files

- `utils/observability.py`
- `tests/locust/e2e.py`
- `tests/locust/common.py`
- `docs/grafana_dashboard.json`
- `docs/evaluation/e2e-observability.md`
- `docker-compose.e2e.yaml`
