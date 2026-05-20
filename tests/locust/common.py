import os
import time

from opentelemetry import metrics
from opentelemetry.metrics import Histogram
from opentelemetry.exporter.otlp.proto.http.metric_exporter import OTLPMetricExporter
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
from opentelemetry.sdk.metrics.view import ExplicitBucketHistogramAggregation, View
from opentelemetry.sdk.resources import Resource

RUN_ID = os.environ.get("LOCUST_RUN_ID", time.strftime("%Y%m%d%H%M%S"))
SECONDS_HISTOGRAM_BUCKETS = (
    0.005,
    0.01,
    0.025,
    0.05,
    0.075,
    0.1,
    0.25,
    0.5,
    0.75,
    1.0,
    2.5,
    5.0,
    7.5,
    10.0,
    25.0,
    50.0,
    75.0,
    100.0,
)

_resource = Resource.create({
    "service.name": os.environ.get("OTEL_SERVICE_NAME", "locust"),
    "service.namespace": "distributed-bookstore",
})
_endpoint = os.environ.get("OTEL_EXPORTER_OTLP_ENDPOINT", "http://localhost:4318")
_provider = MeterProvider(
    resource=_resource,
    metric_readers=[
        PeriodicExportingMetricReader(
            OTLPMetricExporter(endpoint=f"{_endpoint}/v1/metrics"),
            export_interval_millis=1000,
        )
    ],
    views=[
        View(
            instrument_type=Histogram,
            instrument_name="*_seconds",
            aggregation=ExplicitBucketHistogramAggregation(boundaries=SECONDS_HISTOGRAM_BUCKETS),
        )
    ],
)
metrics.set_meter_provider(_provider)
_meter = metrics.get_meter("locust-e2e")

scenario_attempts = _meter.create_counter(
    "locust_scenario_attempts_total",
    description="Total number of Locust scenario attempts",
)
scenario_outcomes = _meter.create_counter(
    "locust_scenario_outcomes_total",
    description="Total number of Locust scenario terminal outcomes",
)
scenario_failures = _meter.create_counter(
    "locust_scenario_failures_total",
    description="Total number of failed Locust E2E lifecycle checks",
)
active_users = _meter.create_up_down_counter(
    "locust_active_users",
    description="Current number of active Locust users",
)
e2e_duration = _meter.create_histogram(
    "locust_e2e_duration_seconds",
    description="Client-observed checkout lifecycle duration",
    unit="s",
)


def mark_user_started(scenario_name):
    active_users.add(1, {"run_id": RUN_ID, "scenario": scenario_name})


def mark_user_stopped(scenario_name):
    active_users.add(-1, {"run_id": RUN_ID, "scenario": scenario_name})


def test_headers(scenario_name):
    return {
        "X-Test-Run-Id": RUN_ID,
        "X-Scenario-Name": scenario_name,
        "Content-Type": "application/json",
    }


def fire_e2e_event(user, name, started_at, scenario_name, outcome, exception=None):
    duration_ms = (time.time() - started_at) * 1000
    attrs = {"run_id": RUN_ID, "scenario": scenario_name, "outcome": outcome}
    scenario_outcomes.add(1, attrs)
    e2e_duration.record(duration_ms / 1000, attrs)
    if exception is not None:
        scenario_failures.add(1, attrs)
    user.environment.events.request.fire(
        request_type="E2E",
        name=name,
        response_time=duration_ms,
        response_length=0,
        response=None,
        context=attrs,
        exception=exception,
    )


def place_order_and_wait(user, payload, scenario_name, expected_terminal="committed", timeout=30):
    started_at = time.time()
    expected_terminals = (
        set(expected_terminal)
        if isinstance(expected_terminal, (list, tuple, set))
        else {expected_terminal}
    )
    scenario_attempts.add(1, {"run_id": RUN_ID, "scenario": scenario_name})

    with user.client.post(
        "/checkout",
        json=payload,
        headers=test_headers(scenario_name),
        catch_response=True,
    ) as response:
        if not response.ok:
            response.failure(f"HTTP {response.status_code}")
            fire_e2e_event(
                user, "checkout_http_error", started_at, scenario_name, "error",
                RuntimeError(f"HTTP {response.status_code}"),
            )
            return

        body = response.json()
        order_id = body.get("orderId")
        status = body.get("status")

        if "denied" in expected_terminals:
            if status == "Order Denied":
                response.success()
                fire_e2e_event(user, "checkout_to_denied", started_at, scenario_name, "denied")
                return
            if expected_terminals == {"denied"}:
                response.failure(f"Expected denial, got {status}")
                fire_e2e_event(
                    user, "checkout_unexpected_terminal", started_at, scenario_name, "error",
                    RuntimeError(f"Expected denied, got {status}"),
                )
                return

        if status == "Order Denied":
            response.failure(f"Denied: {body.get('reason')}")
            fire_e2e_event(
                user, "checkout_to_denied", started_at, scenario_name, "denied",
                RuntimeError(body.get("reason", "denied")),
            )
            return

        if not order_id:
            response.failure("Missing orderId in checkout response")
            fire_e2e_event(
                user, "checkout_missing_order_id", started_at, scenario_name, "error",
                RuntimeError("missing orderId"),
            )
            return

    terminal = poll_until_terminal(user, order_id, scenario_name, timeout=timeout)
    if terminal in expected_terminals:
        fire_e2e_event(user, f"checkout_to_{terminal}", started_at, scenario_name, terminal)
        return

    if terminal == "timeout":
        fire_e2e_event(
            user, "checkout_timeout", started_at, scenario_name, "timeout",
            TimeoutError(f"{order_id} did not reach {sorted(expected_terminals)} within {timeout}s"),
        )
        return

    fire_e2e_event(
        user, f"checkout_unexpected_{terminal}", started_at, scenario_name, terminal,
        RuntimeError(f"expected {sorted(expected_terminals)}, got {terminal}"),
    )


def poll_until_terminal(user, order_id, scenario_name, timeout=30):
    deadline = time.time() + timeout
    while time.time() < deadline:
        time.sleep(1.5)
        with user.client.get(
            f"/order-status/{order_id}",
            name="/order-status/[id]",
            headers=test_headers(scenario_name),
            catch_response=True,
        ) as response:
            if not response.ok:
                response.failure(f"Poll HTTP {response.status_code}")
                return "error"
            state = response.json().get("state")
            if state in ("committed", "aborted"):
                response.success()
                return state
            response.success()
    return "timeout"
