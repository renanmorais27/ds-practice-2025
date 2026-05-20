"""Dummy payment service — a 2PC participant for the executor-led commitment protocol.

Prepare stages a tentative charge, Commit executes the (dummy) side-effect, Abort
drops staged state. All three are idempotent on repeated or out-of-order messages
for the same order_id.
"""

import sys
import os
import threading
import logging
import time
from concurrent import futures

import grpc
from opentelemetry import metrics

FILE = __file__ if "__file__" in globals() else os.getenv("PYTHONFILE", "")

pay_grpc_path = os.path.abspath(os.path.join(FILE, "../../../utils/pb/payment"))
sys.path.insert(0, pay_grpc_path)
import payment_pb2 as payment_pb2
import payment_pb2_grpc as payment_pb2_grpc

project_root = os.path.abspath(os.path.join(FILE, "../../.."))
sys.path.insert(0, project_root)
from utils.observability import configure_otel, record_exception, server_span

logging.basicConfig(level=logging.INFO)

# For demo/testing: set PAYMENT_VOTE_NO=1 to force Prepare votes to fail — exercises abort path.
FORCE_VOTE_NO = os.environ.get("PAYMENT_VOTE_NO", "0") == "1"

STAGED, COMMITTED, ABORTED = "staged", "committed", "aborted"

tracer, meter = configure_otel(os.environ.get("OTEL_SERVICE_NAME", "payment"))
payment_operations_counter = meter.create_counter(
    "payment_operations_total",
    description="Total number of payment participant operations",
)
payment_operation_duration = meter.create_histogram(
    "payment_operation_duration_seconds",
    description="Duration of payment participant operations in seconds",
    unit="s",
)


class PaymentServicer(payment_pb2_grpc.PaymentServiceServicer):
    def __init__(self):
        self._lock = threading.Lock()
        # order_id -> (state, amount). Finalized entries stay as tombstones so
        # Commit/Abort retries after a decision are idempotent no-ops.
        self._tx = {}

    def Prepare(self, request, context):
        started = time.time()
        with server_span(tracer, context, "payment.prepare", **{"order.id": request.order_id}) as span:
            try:
                if FORCE_VOTE_NO:
                    logging.info("[payment] PAYMENT_VOTE_NO=1 — voting NO for order %s", request.order_id)
                    with self._lock:
                        self._tx[request.order_id] = (ABORTED, 0)
                    payment_operations_counter.add(1, {"operation": "prepare", "result": "forced_no"})
                    payment_operation_duration.record(
                        time.time() - started, {"operation": "prepare", "result": "forced_no"}
                    )
                    return payment_pb2.PrepareResponse(ready=False, reason="forced NO vote")

                with self._lock:
                    prior = self._tx.get(request.order_id)
                    if prior is not None:
                        state, _ = prior
                        result = "idempotent_staged" if state == STAGED else "idempotent_finalized"
                        payment_operations_counter.add(1, {"operation": "prepare", "result": result})
                        payment_operation_duration.record(
                            time.time() - started, {"operation": "prepare", "result": result}
                        )
                        return payment_pb2.PrepareResponse(ready=(state == STAGED))
                    self._tx[request.order_id] = (STAGED, request.amount)

                logging.info("[payment] Prepared order %s for $%d", request.order_id, request.amount)
                payment_operations_counter.add(1, {"operation": "prepare", "result": "ready"})
                payment_operation_duration.record(
                    time.time() - started, {"operation": "prepare", "result": "ready"}
                )
                return payment_pb2.PrepareResponse(ready=True)
            except Exception as exc:
                payment_operations_counter.add(1, {"operation": "prepare", "result": "error"})
                payment_operation_duration.record(
                    time.time() - started, {"operation": "prepare", "result": "error"}
                )
                if hasattr(span, "record_exception"):
                    record_exception(span, exc)
                raise

    def Commit(self, request, context):
        started = time.time()
        with server_span(tracer, context, "payment.commit", **{"order.id": request.order_id}) as span:
            try:
                with self._lock:
                    entry = self._tx.get(request.order_id)
                    if entry is None or entry[0] != STAGED:
                        logging.info(
                            "[payment] Commit for %s is a no-op (entry=%s)",
                            request.order_id, entry,
                        )
                        payment_operations_counter.add(1, {"operation": "commit", "result": "noop"})
                        payment_operation_duration.record(
                            time.time() - started, {"operation": "commit", "result": "noop"}
                        )
                        return payment_pb2.CommitResponse(success=True)
                    _, amount = entry
                    self._tx[request.order_id] = (COMMITTED, amount)

                logging.info("[payment] EXECUTED payment for order %s: $%d", request.order_id, amount)
                payment_operations_counter.add(1, {"operation": "commit", "result": "committed"})
                payment_operation_duration.record(
                    time.time() - started, {"operation": "commit", "result": "committed"}
                )
                return payment_pb2.CommitResponse(success=True)
            except Exception as exc:
                payment_operations_counter.add(1, {"operation": "commit", "result": "error"})
                payment_operation_duration.record(
                    time.time() - started, {"operation": "commit", "result": "error"}
                )
                if hasattr(span, "record_exception"):
                    record_exception(span, exc)
                raise

    def Abort(self, request, context):
        started = time.time()
        with server_span(tracer, context, "payment.abort", **{"order.id": request.order_id}):
            with self._lock:
                entry = self._tx.get(request.order_id)
                if entry is None:
                    self._tx[request.order_id] = (ABORTED, 0)
                elif entry[0] == STAGED:
                    self._tx[request.order_id] = (ABORTED, entry[1])

            logging.info("[payment] Aborted order %s", request.order_id)
            payment_operations_counter.add(1, {"operation": "abort", "result": "aborted"})
            payment_operation_duration.record(
                time.time() - started, {"operation": "abort", "result": "aborted"}
            )
            return payment_pb2.AbortResponse(aborted=True)


def serve():
    servicer = PaymentServicer()

    def _staged_transactions_callback(_options):
        with servicer._lock:
            staged = sum(1 for state, _ in servicer._tx.values() if state == STAGED)
        yield metrics.Observation(staged, {})

    meter.create_observable_gauge(
        "payment_staged_transactions",
        callbacks=[_staged_transactions_callback],
        description="Current number of staged payment transactions",
    )

    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    payment_pb2_grpc.add_PaymentServiceServicer_to_server(servicer, server)
    server.add_insecure_port("[::]:50058")
    logging.info("PaymentService listening on port 50058")
    server.start()
    server.wait_for_termination()


if __name__ == "__main__":
    serve()
