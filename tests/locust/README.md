# Locust E2E Evaluation

These scenarios exercise the bookstore from `POST /checkout` through the final
`/order-status/<orderId>` terminal state. HTTP request stats remain visible, but
the primary correctness signal is the synthetic `E2E` Locust event emitted for
`checkout_to_committed`, `checkout_to_aborted`, `checkout_to_denied`, and
`checkout_timeout`.

Locust also exports OTLP metrics for active users, scenario attempts, terminal
outcomes, failed lifecycle checks, and E2E duration.

## Interactive Run

```bash
docker compose --profile e2e up --build
```

Open Locust at http://localhost:8089 and Grafana at http://localhost:3000.

The compose service uses:

```bash
LOCUST_SCENARIO=happy_non_conflicting
LOCUST_RUN_ID=local
```

Override them when starting the stack:

```bash
LOCUST_SCENARIO=mixed_orders_simultaneous LOCUST_RUN_ID=mixed-local \
  docker compose --profile e2e up --build
```

## Headless Runs

Run against the host-mapped orchestrator:

```bash
LOCUST_SCENARIO=single_non_fraudulent_order LOCUST_RUN_ID=single-smoke \
  locust -f tests/locust/e2e.py \
  --host http://localhost:8081 \
  --headless \
  --users 1 \
  --spawn-rate 1 \
  --run-time 30s \
  --csv tests/locust/results/single_non_fraudulent_order \
  --html tests/locust/results/single_non_fraudulent_order.html
```

## Scenarios

| Scenario | Expected outcome |
| --- | --- |
| `single_non_fraudulent_order` | One valid order reaches `committed`. |
| `happy_non_conflicting` | Multiple valid orders for alternating books reach `committed`. |
| `validation_denied` | Invalid card payload returns synchronous `Order Denied`. |
| `fraud_denied` | Fraudulent user/card payload returns synchronous `Order Denied`. |
| `mixed_orders_simultaneous` | Valid orders commit while fraudulent orders are denied in the same run. |
| `payment_abort` | Requires `PAYMENT_VOTE_NO=1`; accepted orders end as `aborted`. |
| `same_book_conflict_low_stock` | Requires `docker-compose.e2e.yaml`; same-book orders produce one commit and DB-driven aborts. |
| `executor_backlog` | Load above executor capacity raises queue wait, pending orders, and eventual timeouts. |
| `suggestions_degraded` | Suggestions failures remain non-fatal; valid orders still commit. |

## Same-Book Conflict Fixture

Use the E2E compose override to start the primary DB with one copy of
`Distributed systems.`:

```bash
LOCUST_SCENARIO=same_book_conflict_low_stock LOCUST_RUN_ID=conflict-local \
  docker compose -f docker-compose.yaml -f docker-compose.e2e.yaml --profile e2e up --build
```

Then run at least two Locust users. The expected behavior is that one order
commits and decrements stock to `0`; competing orders abort during DB prepare
with `insufficient_stock`.

## Manual Frontend Checkpoint Demo

1. Start the full stack with `docker compose --profile e2e up --build`.
2. Open http://localhost:8080.
3. Submit the prefilled non-fraudulent checkout form.
4. Wait for the UI to show final approval.
5. Copy the `Order ID` shown in the UI.
6. In Grafana, inspect the E2E dashboard and search Tempo traces by `order.id`.
7. In logs, search for that order id to show orchestrator enqueue, executor 2PC,
   payment commit, DB commit, and outcome callback.
