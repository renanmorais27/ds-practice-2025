import sys
import os

FILE = __file__ if "__file__" in globals() else os.getenv("PYTHONFILE", "")

fd_grpc_path = os.path.abspath(os.path.join(FILE, "../../../utils/pb/fraud_detection"))
sys.path.insert(0, fd_grpc_path)
import fraud_detection_pb2 as fd_pb2
import fraud_detection_pb2_grpc as fd_grpc

sg_grpc_path = os.path.abspath(os.path.join(FILE, "../../../utils/pb/suggestions"))
sys.path.insert(0, sg_grpc_path)
import suggestions_pb2 as sg_pb2
import suggestions_pb2_grpc as sg_grpc

tv_grpc_path = os.path.abspath(
    os.path.join(FILE, "../../../utils/pb/transaction_verification")
)
sys.path.insert(0, tv_grpc_path)
import transaction_verification_pb2 as tv_pb2
import transaction_verification_pb2_grpc as tv_grpc

oq_grpc_path = os.path.abspath(os.path.join(FILE, "../../../utils/pb/order_queue"))
sys.path.insert(0, oq_grpc_path)
import order_queue_pb2 as oq_pb2
import order_queue_pb2_grpc as oq_grpc

from flask import Flask, request, jsonify
from flask_cors import CORS
import json
import grpc
import logging
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from uuid import uuid4

project_root = os.path.abspath(os.path.join(FILE, "../../.."))
sys.path.insert(0, project_root)
from utils.observability import (
    client_span,
    configure_otel,
    extract_trace_context,
    inject_trace_metadata,
    record_exception,
    safe_attrs,
)
from utils.vector_clock import (
    EVENT_TRACE_METADATA_KEY,
    ORDER_ID_METADATA_KEY,
    SUGGESTED_BOOKS_METADATA_KEY,
    VECTOR_CLOCK_METADATA_KEY,
    deserialize_clock,
    deserialize_trace,
    merge_clocks,
    metadata_to_dict,
    new_clock,
    record_event,
    serialize_clock,
    tick,
)

logging.basicConfig(level=logging.INFO)

_service_name = os.environ.get("OTEL_SERVICE_NAME", "orchestrator")
tracer, meter = configure_otel(_service_name)

requests_counter = meter.create_counter(
    "requests_counter_total",
    description="Total number of checkout requests processed",
)
checkout_duration = meter.create_histogram(
    "checkout_duration_seconds",
    description="Duration of checkout requests in seconds",
    unit="s",
)
checkout_stage_duration = meter.create_histogram(
    "checkout_stage_duration_seconds",
    description="Duration of major checkout stages in seconds",
    unit="s",
)
order_server_lifecycle_duration = meter.create_histogram(
    "order_server_lifecycle_duration_seconds",
    description="Server-side duration from checkout receipt to final 2PC outcome",
    unit="s",
)
active_orders = meter.create_up_down_counter(
    "active_orders",
    description="Number of orders currently pending execution",
)
order_terminal_counter = meter.create_counter(
    "order_terminal_total",
    description="Total number of orders reaching a terminal state",
)
denied_orders_counter = meter.create_counter(
    "denied_orders_total",
    description="Total number of denied orders",
)


def _metadata(order_id, clock):
    return inject_trace_metadata((
        (ORDER_ID_METADATA_KEY, order_id),
        (VECTOR_CLOCK_METADATA_KEY, serialize_clock(clock)),
    ))


def _deny_response(order_id, reason, vector_clock, event_trace):
    return {
        "orderId": order_id,
        "status": "Order Denied",
        "reason": reason,
        "suggestedBooks": [],
        "vectorClock": vector_clock,
        "eventTrace": event_trace,
    }


# ── Init helpers (one per service) ──


def initialize_transaction_verification(payload, order_id, clock):
    with client_span(tracer, "init.transaction_verification", **{"order.id": order_id}):
        user = payload.get("user", {}) or {}
        cc = payload.get("creditCard", {}) or {}
        billing = payload.get("billingAddress", {}) or {}
        items = payload.get("items", [])

        req = tv_pb2.VerificationRequest(
            name=user.get("name", ""),
            email=user.get("contact", ""),
            card_number=cc.get("number", ""),
            expiration_date=cc.get("expirationDate", "") or cc.get("expiry", ""),
            cvv=cc.get("cvv", ""),
            billing_address=tv_pb2.BillingAddress(
                street=billing.get("street", ""),
                city=billing.get("city", ""),
                state=billing.get("state", ""),
                zip=billing.get("zip", ""),
                country=billing.get("country", ""),
            ),
            items=[
                tv_pb2.BookItem(name=item.get("name", ""), quantity=item.get("quantity", 0))
                for item in items
            ],
        )
        with grpc.insecure_channel("transaction_verification:50052") as channel:
            stub = tv_grpc.TransactionVerificationServiceStub(channel)
            res, call = stub.InitializeVerificationOrder.with_call(
                req, metadata=_metadata(order_id, clock)
            )
    meta = metadata_to_dict(call.trailing_metadata())
    return (
        deserialize_clock(meta.get(VECTOR_CLOCK_METADATA_KEY)),
        deserialize_trace(meta.get(EVENT_TRACE_METADATA_KEY)),
    )


def initialize_fraud_detection(payload, order_id, clock):
    with client_span(tracer, "init.fraud_detection", **{"order.id": order_id}):
        user = payload.get("user", {}) or {}
        cc = payload.get("creditCard", {}) or {}
        billing = payload.get("billingAddress", {}) or {}
        items = payload.get("items", [])
        order_amount = str(sum(item.get("quantity", 0) for item in items))

        req = fd_pb2.FraudRequest(
            card_number=cc.get("number", ""),
            order_amount=order_amount,
            name=user.get("name", ""),
            email=user.get("contact", ""),
            billing_address=fd_pb2.BillingAddress(
                street=billing.get("street", ""),
                city=billing.get("city", ""),
                state=billing.get("state", ""),
                zip=billing.get("zip", ""),
                country=billing.get("country", ""),
            ),
        )
        with grpc.insecure_channel("fraud_detection:50051") as channel:
            stub = fd_grpc.FraudDetectionServiceStub(channel)
            res, call = stub.InitializeFraudOrder.with_call(
                req, metadata=_metadata(order_id, clock)
            )
    meta = metadata_to_dict(call.trailing_metadata())
    return (
        deserialize_clock(meta.get(VECTOR_CLOCK_METADATA_KEY)),
        deserialize_trace(meta.get(EVENT_TRACE_METADATA_KEY)),
    )


def initialize_suggestions(items, order_id, clock):
    with client_span(tracer, "init.suggestions", **{"order.id": order_id}):
        book_items = [
            sg_pb2.BookItem(name=item.get("name", ""), quantity=item.get("quantity", 0))
            for item in items
        ]
        with grpc.insecure_channel("suggestions:50053") as channel:
            stub = sg_grpc.SuggestionsServiceStub(channel)
            res, call = stub.InitializeSuggestionsOrder.with_call(
                sg_pb2.SuggestionsRequest(items=book_items),
                metadata=_metadata(order_id, clock),
            )
    meta = metadata_to_dict(call.trailing_metadata())
    return (
        deserialize_clock(meta.get(VECTOR_CLOCK_METADATA_KEY)),
        deserialize_trace(meta.get(EVENT_TRACE_METADATA_KEY)),
    )


# ── Clear broadcast (bonus) ──


def broadcast_clear(order_id, final_clock):
    """Send ClearOrder to all three services with the final vector clock."""
    def clear_tv():
        started = time.time()
        try:
            with client_span(tracer, "clear.transaction_verification", **{"order.id": order_id}):
                with grpc.insecure_channel("transaction_verification:50052") as ch:
                    stub = tv_grpc.TransactionVerificationServiceStub(ch)
                    stub.ClearVerificationOrder.with_call(
                        tv_pb2.Empty(), metadata=_metadata(order_id, final_clock)
                    )
            checkout_stage_duration.record(
                time.time() - started, {"stage": "clear_transaction_verification", "status": "ok"}
            )
            logging.info("[%s] Clear broadcast: TV cleared", order_id)
        except Exception as e:
            checkout_stage_duration.record(
                time.time() - started, {"stage": "clear_transaction_verification", "status": "error"}
            )
            logging.warning("[%s] Clear broadcast TV failed: %s", order_id, e)

    def clear_fd():
        started = time.time()
        try:
            with client_span(tracer, "clear.fraud_detection", **{"order.id": order_id}):
                with grpc.insecure_channel("fraud_detection:50051") as ch:
                    stub = fd_grpc.FraudDetectionServiceStub(ch)
                    stub.ClearFraudOrder.with_call(
                        fd_pb2.Empty(), metadata=_metadata(order_id, final_clock)
                    )
            checkout_stage_duration.record(
                time.time() - started, {"stage": "clear_fraud_detection", "status": "ok"}
            )
            logging.info("[%s] Clear broadcast: FD cleared", order_id)
        except Exception as e:
            checkout_stage_duration.record(
                time.time() - started, {"stage": "clear_fraud_detection", "status": "error"}
            )
            logging.warning("[%s] Clear broadcast FD failed: %s", order_id, e)

    def clear_sg():
        started = time.time()
        try:
            with client_span(tracer, "clear.suggestions", **{"order.id": order_id}):
                with grpc.insecure_channel("suggestions:50053") as ch:
                    stub = sg_grpc.SuggestionsServiceStub(ch)
                    stub.ClearSuggestionsOrder.with_call(
                        sg_pb2.Empty(), metadata=_metadata(order_id, final_clock)
                    )
            checkout_stage_duration.record(
                time.time() - started, {"stage": "clear_suggestions", "status": "ok"}
            )
            logging.info("[%s] Clear broadcast: SG cleared", order_id)
        except Exception as e:
            checkout_stage_duration.record(
                time.time() - started, {"stage": "clear_suggestions", "status": "error"}
            )
            logging.warning("[%s] Clear broadcast SG failed: %s", order_id, e)

    with ThreadPoolExecutor(max_workers=3) as pool:
        pool.submit(clear_tv)
        pool.submit(clear_fd)
        pool.submit(clear_sg)


# ── Flask app ──

app = Flask(__name__)
CORS(app, resources={r"/*": {"origins": "*"}})

# In-memory status map populated by /checkout (pending) and updated by the executor's
# /internal/order-outcome callback after 2PC completes. Frontend polls /order-status/<id>.
# This is intentionally single-instance and non-persistent — the prototype has one orchestrator.
order_statuses = {}
_status_lock = threading.Lock()


def _pending_orders_callback(_options):
    with _status_lock:
        count = sum(1 for v in order_statuses.values() if v.get("state") == "pending")
    yield metrics.Observation(count, {})


meter.create_observable_gauge(
    "pending_orders_gauge",
    callbacks=[_pending_orders_callback],
    description="Current number of orders waiting for 2PC outcome",
)


def _set_status(order_id, state, reason="", extra=None):
    with _status_lock:
        entry = order_statuses.get(order_id, {})
        entry.update({"state": state, "reason": reason})
        if extra:
            entry.update(extra)
        order_statuses[order_id] = entry
        return dict(entry)


@app.route("/", methods=["GET"])
def index():
    return "Hello, orchestrator!"


@app.route("/order-status/<order_id>", methods=["GET"])
def order_status(order_id):
    with _status_lock:
        entry = order_statuses.get(order_id)
    if entry is None:
        return jsonify({"state": "unknown"}), 404
    return jsonify(entry)


@app.route("/internal/order-outcome", methods=["POST"])
def order_outcome():
    """Executor callback after 2PC completes — updates the status map."""
    _t0 = time.time()
    try:
        body = json.loads(request.data)
    except Exception:
        return jsonify({"error": "invalid json"}), 400
    order_id = body.get("orderId")
    outcome = body.get("outcome")  # "committed" | "aborted"
    reason = body.get("reason", "")
    if not order_id or outcome not in ("committed", "aborted"):
        return jsonify({"error": "missing orderId or bad outcome"}), 400
    parent = extract_trace_context(request.headers.items())
    with tracer.start_as_current_span("order_outcome", context=parent) as span:
        span.set_attributes(safe_attrs(
            **{
                "order.id": order_id,
                "order.outcome": outcome,
            }
        ))
        entry = _set_status(order_id, outcome, reason, extra={"outcome_at": time.time()})
        scenario = entry.get("scenario", "unknown")
        created_at = entry.get("created_at")
        if created_at:
            order_server_lifecycle_duration.record(
                time.time() - created_at,
                {"job": "orchestrator", "outcome": outcome, "scenario": scenario},
            )
        order_terminal_counter.add(
            1, {"job": "orchestrator", "outcome": outcome, "scenario": scenario}
        )
        checkout_stage_duration.record(
            time.time() - _t0, {"stage": "order_outcome", "status": outcome}
        )
    active_orders.add(-1, {"job": "orchestrator"})
    logging.info("[%s] 2PC outcome recorded: %s (%s)", order_id, outcome, reason)
    return jsonify({"ok": True})


@app.route("/checkout", methods=["POST"])
def checkout():
    """Process a checkout request through a partially ordered pipeline with vector clocks."""
    with tracer.start_as_current_span("checkout") as checkout_span:
        _t0 = time.time()
        request_data = json.loads(request.data)
        run_id = request.headers.get("X-Test-Run-Id", "")
        scenario = request.headers.get("X-Scenario-Name", "manual")
        num_items = len(request_data.get("items", []))
        checkout_span.set_attributes(safe_attrs(
            **{
                "order.items_count": num_items,
                "test.run_id": run_id,
                "scenario.name": scenario,
            }
        ))
        logging.info("Checkout request received with %d items", num_items)

        order_id = str(uuid4())
        checkout_span.set_attribute("order.id", order_id)
        vector_clock = new_clock()
        event_trace = []

        def _record_result(status):
            dur = time.time() - _t0
            requests_counter.add(1, {"job": "orchestrator", "status": status})
            checkout_duration.record(dur, {"job": "orchestrator", "status": status})

        # ── Orchestrator events: receive + create order ID ──
        vector_clock = tick(vector_clock, "orchestrator")
        record_event(event_trace, vector_clock, "orchestrator", "checkout_request_received")
        vector_clock = tick(vector_clock, "orchestrator")
        record_event(event_trace, vector_clock, "orchestrator", "order_id_created")

        # ── Stage 1: Parallel init (same parent clock snapshot for true concurrency) ──
        init_clock = dict(vector_clock)  # snapshot — all three get the SAME clock

        executor = ThreadPoolExecutor(max_workers=4)
        try:
            init_started = time.time()
            with tracer.start_as_current_span("init_services") as init_span:
                init_span.set_attribute("order.id", order_id)
                record_event(event_trace, init_clock, "orchestrator", "dispatch_init_rpcs")

                future_tv = executor.submit(
                    initialize_transaction_verification, request_data, order_id, init_clock
                )
                future_fd = executor.submit(
                    initialize_fraud_detection, request_data, order_id, init_clock
                )
                future_sg = executor.submit(
                    initialize_suggestions, request_data.get("items", []), order_id, init_clock
                )

                init_futures = {
                    future_tv: "transaction_verification",
                    future_fd: "fraud_detection",
                    future_sg: "suggestions",
                }

                # Wait for all inits, fail fast on any error
                for future in as_completed(init_futures):
                    svc = init_futures[future]
                    try:
                        service_clock, service_trace = future.result()
                        event_trace.extend(service_trace)
                        vector_clock = merge_clocks(vector_clock, service_clock)
                        vector_clock = tick(vector_clock, "orchestrator")
                        record_event(
                            event_trace, vector_clock, "orchestrator",
                            f"{svc}_initialized",
                        )
                    except Exception as exc:
                        for f in init_futures:
                            if f is not future:
                                f.cancel()
                        vector_clock = tick(vector_clock, "orchestrator")
                        record_event(
                            event_trace, vector_clock, "orchestrator",
                            f"{svc}_initialization_failed",
                        )
                        logging.error("[%s] Init failed for %s: %s", order_id, svc, exc)
                        _record_result("Order Denied")
                        denied_orders_counter.add(1, {"job": "orchestrator", "reason": "init_failed"})
                        result = _deny_response(
                            order_id,
                            f"{svc} initialization failed: {exc}",
                            vector_clock,
                            event_trace,
                        )
                        broadcast_clear(order_id, vector_clock)
                        return result
            checkout_stage_duration.record(
                time.time() - init_started, {"stage": "init_services", "status": "ok"}
            )

            logging.info("[%s] All services initialized, starting verification flow", order_id)

            # ── Stage 2: Execution via StartVerificationFlow ──
            vector_clock = tick(vector_clock, "orchestrator")
            record_event(
                event_trace, vector_clock, "orchestrator",
                "dispatch_start_verification_flow",
            )

            try:
                verification_started = time.time()
                with client_span(tracer, "verification.start_flow", **{"order.id": order_id}):
                    with grpc.insecure_channel("transaction_verification:50052") as channel:
                        stub = tv_grpc.TransactionVerificationServiceStub(channel)
                        res, call = stub.StartVerificationFlow.with_call(
                            tv_pb2.Empty(), metadata=_metadata(order_id, vector_clock)
                        )
                checkout_stage_duration.record(
                    time.time() - verification_started,
                    {"stage": "verification_flow", "status": "ok"},
                )
                meta = metadata_to_dict(call.trailing_metadata())
                service_clock = deserialize_clock(meta.get(VECTOR_CLOCK_METADATA_KEY))
                service_trace = deserialize_trace(meta.get(EVENT_TRACE_METADATA_KEY))
                books_payload = meta.get(SUGGESTED_BOOKS_METADATA_KEY, "[]")

                event_trace.extend(service_trace)
                vector_clock = merge_clocks(vector_clock, service_clock)
                vector_clock = tick(vector_clock, "orchestrator")

                if not res.is_valid:
                    record_event(
                        event_trace, vector_clock, "orchestrator", "checkout_denied",
                    )
                    _record_result("Order Denied")
                    denied_orders_counter.add(1, {"job": "orchestrator", "reason": "verification_failed"})
                    result = _deny_response(
                        order_id,
                        res.message or "Verification failed",
                        vector_clock,
                        event_trace,
                    )
                    broadcast_clear(order_id, vector_clock)
                    return result

                # Verification passed — hand the order off to the executor for 2PC.
                suggested_books = json.loads(books_payload)
                items = request_data.get("items", [])

                try:
                    enqueue_started = time.time()
                    oq_items = [
                        oq_pb2.BookItem(title=item.get("name", ""), quantity=item.get("quantity", 0))
                        for item in items
                    ]
                    with client_span(tracer, "order_queue.enqueue", **{"order.id": order_id}):
                        with grpc.insecure_channel("order_queue:50054") as oq_channel:
                            oq_stub = oq_grpc.OrderQueueServiceStub(oq_channel)
                            enqueue_res = oq_stub.Enqueue(
                                oq_pb2.EnqueueRequest(orderId=order_id, items=oq_items),
                                metadata=inject_trace_metadata(),
                                timeout=5,
                            )
                            if not enqueue_res.success:
                                raise RuntimeError("Order queue rejected enqueue")
                    checkout_stage_duration.record(
                        time.time() - enqueue_started, {"stage": "enqueue", "status": "ok"}
                    )

                    _set_status(
                        order_id, "pending", "",
                        extra={
                            "suggestedBooks": suggested_books,
                            "created_at": _t0,
                            "accepted_at": time.time(),
                            "run_id": run_id,
                            "scenario": scenario,
                        },
                    )
                    active_orders.add(1, {"job": "orchestrator"})
                    logging.info("[%s] Order enqueued, awaiting 2PC outcome", order_id)
                    vector_clock = tick(vector_clock, "orchestrator")
                    record_event(
                        event_trace, vector_clock, "orchestrator", "order_enqueued",
                    )
                except Exception as enq_exc:
                    logging.error("[%s] Failed to enqueue order: %s", order_id, enq_exc)
                    vector_clock = tick(vector_clock, "orchestrator")
                    record_event(
                        event_trace, vector_clock, "orchestrator", "order_enqueue_failed",
                    )
                    _record_result("Order Denied")
                    denied_orders_counter.add(1, {"job": "orchestrator", "reason": "enqueue_failed"})
                    result = _deny_response(
                        order_id,
                        f"Order could not be enqueued: {enq_exc}",
                        vector_clock,
                        event_trace,
                    )
                    broadcast_clear(order_id, vector_clock)
                    return result

                record_event(
                    event_trace, vector_clock, "orchestrator", "checkout_response_ready",
                )
                _record_result("Order Approved")
                response_started = time.time()
                result = {
                    "orderId": order_id,
                    "status": "accepted",
                    "reason": "Transaction valid; awaiting distributed commit",
                    "suggestedBooks": suggested_books,
                    "vectorClock": vector_clock,
                    "eventTrace": event_trace,
                }
                checkout_stage_duration.record(
                    time.time() - response_started,
                    {"stage": "response_creation", "status": "accepted"},
                )
                broadcast_clear(order_id, vector_clock)
                return result

            except Exception as exc:
                logging.error("[%s] StartVerificationFlow error: %s", order_id, exc)
                vector_clock = tick(vector_clock, "orchestrator")
                record_event(
                    event_trace, vector_clock, "orchestrator",
                    "verification_flow_error",
                )
                _record_result("Order Denied")
                denied_orders_counter.add(1, {"job": "orchestrator", "reason": "verification_error"})
                result = _deny_response(
                    order_id, str(exc), vector_clock, event_trace
                )
                broadcast_clear(order_id, vector_clock)
                return result

        finally:
            executor.shutdown(wait=False, cancel_futures=True)


if __name__ == "__main__":
    app.run(host="0.0.0.0")
