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

db_grpc_path = os.path.abspath(os.path.join(FILE, "../../../utils/pb/books_database"))
sys.path.insert(0, db_grpc_path)
import books_database_pb2 as db_pb2
import books_database_pb2_grpc as db_grpc

from flask import Flask, request
from flask_cors import CORS
import json
import grpc
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from uuid import uuid4

project_root = os.path.abspath(os.path.join(FILE, "../../.."))
sys.path.insert(0, project_root)
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


def _metadata(order_id, clock):
    return (
        (ORDER_ID_METADATA_KEY, order_id),
        (VECTOR_CLOCK_METADATA_KEY, serialize_clock(clock)),
    )


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
        try:
            with grpc.insecure_channel("transaction_verification:50052") as ch:
                stub = tv_grpc.TransactionVerificationServiceStub(ch)
                stub.ClearVerificationOrder.with_call(
                    tv_pb2.Empty(), metadata=_metadata(order_id, final_clock)
                )
            logging.info("[%s] Clear broadcast: TV cleared", order_id)
        except Exception as e:
            logging.warning("[%s] Clear broadcast TV failed: %s", order_id, e)

    def clear_fd():
        try:
            with grpc.insecure_channel("fraud_detection:50051") as ch:
                stub = fd_grpc.FraudDetectionServiceStub(ch)
                stub.ClearFraudOrder.with_call(
                    fd_pb2.Empty(), metadata=_metadata(order_id, final_clock)
                )
            logging.info("[%s] Clear broadcast: FD cleared", order_id)
        except Exception as e:
            logging.warning("[%s] Clear broadcast FD failed: %s", order_id, e)

    def clear_sg():
        try:
            with grpc.insecure_channel("suggestions:50053") as ch:
                stub = sg_grpc.SuggestionsServiceStub(ch)
                stub.ClearSuggestionsOrder.with_call(
                    sg_pb2.Empty(), metadata=_metadata(order_id, final_clock)
                )
            logging.info("[%s] Clear broadcast: SG cleared", order_id)
        except Exception as e:
            logging.warning("[%s] Clear broadcast SG failed: %s", order_id, e)

    with ThreadPoolExecutor(max_workers=3) as pool:
        pool.submit(clear_tv)
        pool.submit(clear_fd)
        pool.submit(clear_sg)


# ── Flask app ──

app = Flask(__name__)
CORS(app, resources={r"/*": {"origins": "*"}})


@app.route("/", methods=["GET"])
def index():
    return "Hello, orchestrator!"


@app.route("/checkout", methods=["POST"])
def checkout():
    """Process a checkout request through a partially ordered pipeline with vector clocks."""
    request_data = json.loads(request.data)
    logging.info(
        "Checkout request received with %d items",
        len(request_data.get("items", [])),
    )

    order_id = str(uuid4())
    vector_clock = new_clock()
    event_trace = []

    # ── Orchestrator events: receive + create order ID ──
    vector_clock = tick(vector_clock, "orchestrator")
    record_event(event_trace, vector_clock, "orchestrator", "checkout_request_received")
    vector_clock = tick(vector_clock, "orchestrator")
    record_event(event_trace, vector_clock, "orchestrator", "order_id_created")

    # ── Stage 1: Parallel init (same parent clock snapshot for true concurrency) ──
    init_clock = dict(vector_clock)  # snapshot — all three get the SAME clock

    executor = ThreadPoolExecutor(max_workers=4)
    try:
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
            service_name = init_futures[future]
            try:
                service_clock, service_trace = future.result()
                event_trace.extend(service_trace)
                vector_clock = merge_clocks(vector_clock, service_clock)
                vector_clock = tick(vector_clock, "orchestrator")
                record_event(
                    event_trace, vector_clock, "orchestrator",
                    f"{service_name}_initialized",
                )
            except Exception as exc:
                # Cancel remaining init futures
                for f in init_futures:
                    if f is not future:
                        f.cancel()
                vector_clock = tick(vector_clock, "orchestrator")
                record_event(
                    event_trace, vector_clock, "orchestrator",
                    f"{service_name}_initialization_failed",
                )
                logging.error("[%s] Init failed for %s: %s", order_id, service_name, exc)
                result = _deny_response(
                    order_id,
                    f"{service_name} initialization failed: {exc}",
                    vector_clock,
                    event_trace,
                )
                # Clear broadcast even on init failure
                broadcast_clear(order_id, vector_clock)
                return result

        logging.info("[%s] All services initialized, starting verification flow", order_id)

        # ── Stage 2: Execution via StartVerificationFlow ──
        vector_clock = tick(vector_clock, "orchestrator")
        record_event(
            event_trace, vector_clock, "orchestrator",
            "dispatch_start_verification_flow",
        )

        try:
            with grpc.insecure_channel("transaction_verification:50052") as channel:
                stub = tv_grpc.TransactionVerificationServiceStub(channel)
                res, call = stub.StartVerificationFlow.with_call(
                    tv_pb2.Empty(), metadata=_metadata(order_id, vector_clock)
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
                result = _deny_response(
                    order_id,
                    res.message or "Verification failed",
                    vector_clock,
                    event_trace,
                )
                broadcast_clear(order_id, vector_clock)
                return result

            # Success — atomically reserve stock before enqueueing
            suggested_books = json.loads(books_payload)

            items = request_data.get("items", [])
            reserved = []  # tracks (title, quantity) for rollback on any later failure

            def release_reserved_stock():
                try:
                    with grpc.insecure_channel("books_db_1:50060") as rb_channel:
                        rb_stub = db_grpc.BooksDatabaseStub(rb_channel)
                        for r_title, r_qty in reserved:
                            try:
                                rb_stub.Increment(
                                    db_pb2.IncrementRequest(title=r_title, quantity=r_qty), timeout=5
                                )
                            except Exception as rb_exc:
                                logging.error("[%s] Rollback failed for '%s': %s", order_id, r_title, rb_exc)
                except Exception as e:
                    logging.error("[%s] Could not open DB channel for rollback: %s", order_id, e)

            try:
                with grpc.insecure_channel("books_db_1:50060") as db_channel:
                    db_stub = db_grpc.BooksDatabaseStub(db_channel)
                    deny_reason = ""
                    for item in items:
                        title = item.get("name", "")
                        quantity = item.get("quantity", 0)
                        resp = db_stub.TryDecrement(
                            db_pb2.TryDecrementRequest(title=title, quantity=quantity), timeout=5
                        )
                        if not resp.success:
                            deny_reason = f"Insufficient stock for '{title}' (requested: {quantity})"
                            break
                        reserved.append((title, quantity))

                    if deny_reason:
                        release_reserved_stock()
                        vector_clock = tick(vector_clock, "orchestrator")
                        record_event(event_trace, vector_clock, "orchestrator", "stock_check_failed")
                        result = _deny_response(order_id, deny_reason, vector_clock, event_trace)
                        broadcast_clear(order_id, vector_clock)
                        return result

                vector_clock = tick(vector_clock, "orchestrator")
                record_event(event_trace, vector_clock, "orchestrator", "stock_reserved")
            except Exception as stock_exc:
                logging.warning("[%s] Stock reservation failed: %s", order_id, stock_exc)
                release_reserved_stock()

            # Enqueue the approved order and require confirmation before responding.
            try:
                oq_items = [
                    oq_pb2.BookItem(title=item.get("name", ""), quantity=item.get("quantity", 0))
                    for item in items
                ]
                with grpc.insecure_channel("order_queue:50054") as oq_channel:
                    oq_stub = oq_grpc.OrderQueueServiceStub(oq_channel)
                    enqueue_res = oq_stub.Enqueue(
                        oq_pb2.EnqueueRequest(orderId=order_id, items=oq_items), timeout=5
                    )
                    if not enqueue_res.success:
                        raise RuntimeError("Order queue rejected enqueue")

                logging.info("[%s] Order enqueued successfully", order_id)
                vector_clock = tick(vector_clock, "orchestrator")
                record_event(
                    event_trace, vector_clock, "orchestrator", "order_enqueued",
                )
            except Exception as enq_exc:
                logging.error("[%s] Failed to enqueue order: %s", order_id, enq_exc)
                release_reserved_stock()
                vector_clock = tick(vector_clock, "orchestrator")
                record_event(
                    event_trace, vector_clock, "orchestrator", "order_enqueue_failed",
                )
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

            result = {
                "orderId": order_id,
                "status": "Order Approved",
                "reason": "Transaction valid",
                "suggestedBooks": suggested_books,
                "vectorClock": vector_clock,
                "eventTrace": event_trace,
            }
            broadcast_clear(order_id, vector_clock)
            return result

        except Exception as exc:
            logging.error("[%s] StartVerificationFlow error: %s", order_id, exc)
            vector_clock = tick(vector_clock, "orchestrator")
            record_event(
                event_trace, vector_clock, "orchestrator",
                "verification_flow_error",
            )
            result = _deny_response(
                order_id, str(exc), vector_clock, event_trace
            )
            broadcast_clear(order_id, vector_clock)
            return result

    finally:
        executor.shutdown(wait=False, cancel_futures=True)


if __name__ == "__main__":
    app.run(host="0.0.0.0")
