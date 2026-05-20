# E2E Observability Evaluation

This guide ties the automated Locust runs, manual frontend demo, and Grafana
dashboard together for checkpoint evaluation.

## Start The Stack

```bash
docker compose --profile e2e up --build
```

Open:

- Frontend: http://localhost:8080
- Orchestrator: http://localhost:8081
- Grafana: http://localhost:3000 (`admin` / `admin`)
- Locust: http://localhost:8089

If Grafana does not show the dashboard automatically, import
`docs/grafana_dashboard.json` from **Dashboards > New > Import**.

Locust exports `locust_active_users`, `locust_scenario_attempts_total`,
`locust_scenario_outcomes_total`, `locust_scenario_failures_total`, and
`locust_e2e_duration_seconds` through OTLP, so Grafana can separate load,
expected outcomes, failures, and client-observed latency by run and scenario.

## Manual Single-Order Demo

1. Open http://localhost:8080.
2. Submit the prefilled valid order form.
3. Wait until the UI shows the final approved result.
4. Copy the displayed `Order ID`.
5. Search service logs for the order id:

```bash
docker compose logs orchestrator executor_3 payment books_db_1 | grep "<order-id>"
```

Expected log evidence:

- orchestrator received checkout and enqueued the order
- executor dequeued the order and drove 2PC
- payment committed
- books DB committed and decremented stock
- executor notified orchestrator
- frontend polling resolved to `committed`

In Grafana, check:

- `Total Checkout Requests`
- `Server Order Lifecycle (p95)`
- `Executor & 2PC Outcomes`
- `Payment Operations`
- `DB Operations`
- Tempo trace search by `order.id`

## Automated Scenario Matrix

| Scenario | Run setup | Healthy evidence |
| --- | --- | --- |
| `single_non_fraudulent_order` | 1 user, 1 spawn rate | One `checkout_to_committed` E2E event. |
| `happy_non_conflicting` | 3 users, 1 spawn rate | Mostly/all committed outcomes, stable queue depth. |
| `validation_denied` | 1+ users | `Order Denied` with validation reason, no queue/2PC activity. |
| `fraud_denied` | 1+ users | `Order Denied` with fraud reason, no queue/2PC activity. |
| `mixed_orders_simultaneous` | 4+ users | Interleaved `committed` and `denied` outcomes in one run. |
| `payment_abort` | set `PAYMENT_VOTE_NO=1` for payment | Accepted checkout, payment NO vote, `aborted` outcome, no stock decrement. |
| `same_book_conflict_low_stock` | use `docker-compose.e2e.yaml` and 2+ users | One commit, one or more DB `insufficient_stock` aborts, final stock `0`. |
| `executor_backlog` | above known executor capacity | Rising queue wait, pending orders, and E2E timeout events. |
| `suggestions_degraded` | stop/break suggestions service | Suggestions error/fallback, valid order still commits. |

## Headless Examples

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

```bash
LOCUST_SCENARIO=mixed_orders_simultaneous LOCUST_RUN_ID=mixed-local \
  locust -f tests/locust/e2e.py \
  --host http://localhost:8081 \
  --headless \
  --users 4 \
  --spawn-rate 4 \
  --run-time 60s \
  --csv tests/locust/results/mixed_orders_simultaneous \
  --html tests/locust/results/mixed_orders_simultaneous.html
```

## Same-Book Conflict

Start with the low-stock override:

```bash
LOCUST_SCENARIO=same_book_conflict_low_stock LOCUST_RUN_ID=conflict-local \
  docker compose -f docker-compose.yaml -f docker-compose.e2e.yaml --profile e2e up --build
```

Then start a 2+ user Locust run. The easiest reset is to restart with a fresh
container for `books_db_1`, because the fixture is a startup-only initial stock
override.

## OpenTelemetry Instrument Checklist

The implementation intentionally includes at least two examples of each required
instrument type:

| Instrument type | Examples |
| --- | --- |
| Span | `checkout`, `transaction_verification.*`, `2pc.finalize`, `payment.commit`, `db.commit` |
| Counter | `requests_counter_total`, `order_terminal_total`, `two_pc_votes_total`, `payment_operations_total` |
| UpDownCounter | `active_orders`, `staged_transactions`, `executor_active_orders` |
| Histogram | `checkout_duration_seconds`, `order_server_lifecycle_duration_seconds`, `order_queue_wait_seconds`, `two_pc_phase_duration_seconds` |
| Asynchronous Gauge | `pending_orders_gauge`, `order_queue_depth`, `book_stock_level`, `payment_staged_transactions`, `executor_is_leader` |

## Troubleshooting

- Empty Grafana panels: wait a few seconds for OTLP export and Prometheus scrape intervals, then rerun traffic.
- No traces: confirm services have `OTEL_EXPORTER_OTLP_ENDPOINT=http://observability:4318`.
- Locust timeouts: inspect `order_queue_depth`, `pending_orders_gauge`, executor dequeue metrics, and executor logs.
- Suggestions errors: expected without `GOOGLE_API_KEY`; suggestions should fall back and remain non-fatal.
- Conflict scenario does not abort: ensure the E2E compose override is active and `books_db_1` started fresh.
