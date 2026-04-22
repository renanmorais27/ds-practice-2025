"""Dummy payment service — a 2PC participant for the executor-led commitment protocol.

Prepare stages a tentative charge, Commit executes the (dummy) side-effect, Abort
drops staged state. All three are idempotent on repeated or out-of-order messages
for the same order_id.
"""

import sys
import os
import threading
import logging
from concurrent import futures

import grpc

FILE = __file__ if "__file__" in globals() else os.getenv("PYTHONFILE", "")

pay_grpc_path = os.path.abspath(os.path.join(FILE, "../../../utils/pb/payment"))
sys.path.insert(0, pay_grpc_path)
import payment_pb2 as payment_pb2
import payment_pb2_grpc as payment_pb2_grpc

logging.basicConfig(level=logging.INFO)

# For demo/testing: set PAYMENT_VOTE_NO=1 to force Prepare votes to fail — exercises abort path.
FORCE_VOTE_NO = os.environ.get("PAYMENT_VOTE_NO", "0") == "1"

STAGED, COMMITTED, ABORTED = "staged", "committed", "aborted"


class PaymentServicer(payment_pb2_grpc.PaymentServiceServicer):
    def __init__(self):
        self._lock = threading.Lock()
        # order_id -> (state, amount). Finalized entries stay as tombstones so
        # Commit/Abort retries after a decision are idempotent no-ops.
        self._tx = {}

    def Prepare(self, request, context):
        if FORCE_VOTE_NO:
            logging.info("[payment] PAYMENT_VOTE_NO=1 — voting NO for order %s", request.order_id)
            with self._lock:
                self._tx[request.order_id] = (ABORTED, 0)
            return payment_pb2.PrepareResponse(ready=False, reason="forced NO vote")

        with self._lock:
            prior = self._tx.get(request.order_id)
            if prior is not None:
                state, _ = prior
                # Idempotent re-Prepare: return whatever verdict this order is already in.
                return payment_pb2.PrepareResponse(ready=(state == STAGED))
            self._tx[request.order_id] = (STAGED, request.amount)

        logging.info("[payment] Prepared order %s for $%d", request.order_id, request.amount)
        return payment_pb2.PrepareResponse(ready=True)

    def Commit(self, request, context):
        with self._lock:
            entry = self._tx.get(request.order_id)
            if entry is None or entry[0] != STAGED:
                # Unknown or already finalized — idempotent no-op.
                logging.info(
                    "[payment] Commit for %s is a no-op (entry=%s)",
                    request.order_id, entry,
                )
                return payment_pb2.CommitResponse(success=True)
            _, amount = entry
            self._tx[request.order_id] = (COMMITTED, amount)

        logging.info("[payment] EXECUTED payment for order %s: $%d", request.order_id, amount)
        return payment_pb2.CommitResponse(success=True)

    def Abort(self, request, context):
        with self._lock:
            entry = self._tx.get(request.order_id)
            if entry is None:
                # Tombstone for a possible late Prepare retry.
                self._tx[request.order_id] = (ABORTED, 0)
            elif entry[0] == STAGED:
                self._tx[request.order_id] = (ABORTED, entry[1])
            # Already ABORTED or COMMITTED: leave alone, idempotent.

        logging.info("[payment] Aborted order %s", request.order_id)
        return payment_pb2.AbortResponse(aborted=True)


def serve():
    servicer = PaymentServicer()
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    payment_pb2_grpc.add_PaymentServiceServicer_to_server(servicer, server)
    server.add_insecure_port("[::]:50058")
    logging.info("PaymentService listening on port 50058")
    server.start()
    server.wait_for_termination()


if __name__ == "__main__":
    serve()
