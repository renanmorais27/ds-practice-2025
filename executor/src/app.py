import sys
import os
import time
import threading
import logging
import urllib.request
import urllib.error
import json
from concurrent import futures

import grpc

FILE = __file__ if "__file__" in globals() else os.getenv("PYTHONFILE", "")

# Import executor proto stubs
ex_grpc_path = os.path.abspath(os.path.join(FILE, "../../../utils/pb/executor"))
sys.path.insert(0, ex_grpc_path)
import executor_pb2 as ex_pb2
import executor_pb2_grpc as ex_grpc

# Import order_queue proto stubs (executor is a client of the queue)
oq_grpc_path = os.path.abspath(os.path.join(FILE, "../../../utils/pb/order_queue"))
sys.path.insert(0, oq_grpc_path)
import order_queue_pb2 as oq_pb2
import order_queue_pb2_grpc as oq_grpc

# Import books_database proto stubs (2PC participant)
db_grpc_path = os.path.abspath(os.path.join(FILE, "../../../utils/pb/books_database"))
sys.path.insert(0, db_grpc_path)
import books_database_pb2 as db_pb2
import books_database_pb2_grpc as db_grpc

# Import payment proto stubs (2PC participant)
pay_grpc_path = os.path.abspath(os.path.join(FILE, "../../../utils/pb/payment"))
sys.path.insert(0, pay_grpc_path)
import payment_pb2 as payment_pb2
import payment_pb2_grpc as payment_pb2_grpc


logging.basicConfig(level=logging.INFO)

# Configuration from environment
EXECUTOR_ID = int(os.environ.get("EXECUTOR_ID", "1"))
PEERS = os.environ.get("PEERS", "")  # comma-separated "id@host:port"
ORDER_QUEUE_ADDR = os.environ.get("ORDER_QUEUE_ADDR", "order_queue:50054")
BOOKS_DB_ADDR = os.environ.get("BOOKS_DB_ADDR", "books_db_1:50060")
PAYMENT_ADDR = os.environ.get("PAYMENT_ADDR", "payment:50058")
ORCHESTRATOR_OUTCOME_URL = os.environ.get(
    "ORCHESTRATOR_OUTCOME_URL", "http://orchestrator:5000/internal/order-outcome",
)

ELECTION_TIMEOUT = 2  # seconds to wait for response from higher-ID peers
DEQUEUE_INTERVAL = 3  # seconds between dequeue attempts
HEARTBEAT_INTERVAL = 5  # seconds between leader liveness checks
PREPARE_TIMEOUT = 5   # seconds per participant Prepare call
FINALIZE_TIMEOUT = 5  # seconds per participant Commit/Abort call
FINALIZE_RETRIES = 3  # bounded retries for Commit/Abort on transient failure
FINALIZE_BACKOFF = 0.5  # seconds between retries


def _dummy_amount(items):
    """Dummy charge amount in cents: 1000 per unit. Payment service doesn't validate."""
    return sum(i.quantity for i in items) * 1000


def parse_peers():
    """Parse PEERS env var into list of (id, address) tuples.

    Format: 'id@host:port,id@host:port' e.g. '2@executor_2:50055,3@executor_3:50055'
    """
    if not PEERS:
        return []
    result = []
    for entry in PEERS.split(","):
        entry = entry.strip()
        if "@" in entry:
            peer_id_str, addr = entry.split("@", 1)
            result.append((int(peer_id_str), addr))
    return result


class ExecutorServiceServicer(ex_grpc.ExecutorServiceServicer):
    """Handles incoming Election and Victory RPCs from peer executors."""

    def __init__(self, executor_node):
        self.node = executor_node

    def Election(self, request, context):
        """Respond to election request from a lower-ID node."""
        logging.info(
            "[Executor %d] Received Election from candidate %d",
            self.node.executor_id, request.candidateId,
        )
        # If we receive an election message, we're alive — respond and start our own election
        if request.candidateId < self.node.executor_id:
            threading.Thread(target=self.node.start_election, daemon=True).start()
        return ex_pb2.ElectionResponse(alive=True)

    def Victory(self, request, context):
        """Accept a victory declaration from the new leader."""
        logging.info(
            "[Executor %d] Received Victory from leader %d",
            self.node.executor_id, request.leaderId,
        )
        self.node.leader_id = request.leaderId
        return ex_pb2.VictoryResponse(acknowledged=True)


class ExecutorNode:
    """Implements the bully algorithm for leader election and order execution."""

    def __init__(self):
        self.executor_id = EXECUTOR_ID
        self.peers = parse_peers()
        self._state_lock = threading.Lock()
        self._leader_id = None
        self._election_lock = threading.Lock()
        self._electing = False

    # Fix 2: Thread-safe leader_id via property
    @property
    def leader_id(self):
        with self._state_lock:
            return self._leader_id

    @leader_id.setter
    def leader_id(self, value):
        with self._state_lock:
            self._leader_id = value

    def higher_peers(self):
        """Return peers with IDs higher than ours."""
        return [(pid, addr) for pid, addr in self.peers if pid > self.executor_id]

    # Fix 1+5: Iterative loop with try/finally for _electing flag
    def start_election(self):
        """Bully algorithm: contact all higher-ID peers. Iterative, not recursive."""
        while True:
            with self._election_lock:
                if self._electing:
                    return
                self._electing = True

            try:
                logging.info("[Executor %d] Starting election...", self.executor_id)
                higher = self.higher_peers()

                if not higher:
                    self._declare_victory()
                    return

                # Send Election to all higher-ID peers
                any_alive = False
                for peer_id, addr in higher:
                    try:
                        with grpc.insecure_channel(addr) as channel:
                            stub = ex_grpc.ExecutorServiceStub(channel)
                            response = stub.Election(
                                ex_pb2.ElectionRequest(candidateId=self.executor_id),
                                timeout=ELECTION_TIMEOUT,
                            )
                            if response.alive:
                                any_alive = True
                                logging.info(
                                    "[Executor %d] Peer %d is alive, standing down",
                                    self.executor_id, peer_id,
                                )
                    except Exception:
                        logging.info(
                            "[Executor %d] Peer %d unreachable",
                            self.executor_id, peer_id,
                        )

                if not any_alive:
                    self._declare_victory()
                    return

                # Wait for a Victory message from a higher-ID peer
                logging.info(
                    "[Executor %d] Waiting for Victory from higher-ID peer...",
                    self.executor_id,
                )
            finally:
                with self._election_lock:
                    self._electing = False

            # Sleep outside the try/finally so _electing is already reset
            time.sleep(ELECTION_TIMEOUT * 2)
            if self.leader_id is not None:
                return  # Victory received, done
            # Otherwise loop back to retry

    def _declare_victory(self):
        """Broadcast Victory to all peers and become leader."""
        self.leader_id = self.executor_id

        logging.info("[Executor %d] I am the leader!", self.executor_id)

        # Fix 7: Use self.peers directly instead of all_peers()
        for peer_id, addr in self.peers:
            try:
                with grpc.insecure_channel(addr) as channel:
                    stub = ex_grpc.ExecutorServiceStub(channel)
                    stub.Victory(
                        ex_pb2.VictoryRequest(leaderId=self.executor_id),
                        timeout=ELECTION_TIMEOUT,
                    )
            except Exception:
                logging.warning(
                    "[Executor %d] Could not notify peer %d of victory",
                    self.executor_id, peer_id,
                )

    # Fix 4: Use Victory probe instead of Election for liveness check
    def check_leader_alive(self):
        """Check if the current leader is still reachable via a Victory probe."""
        if self.leader_id is None or self.leader_id == self.executor_id:
            return True

        for peer_id, addr in self.peers:
            if peer_id == self.leader_id:
                try:
                    with grpc.insecure_channel(addr) as channel:
                        stub = ex_grpc.ExecutorServiceStub(channel)
                        stub.Victory(
                            ex_pb2.VictoryRequest(leaderId=self.leader_id),
                            timeout=ELECTION_TIMEOUT,
                        )
                    return True
                except Exception:
                    logging.warning(
                        "[Executor %d] Leader %d unreachable!",
                        self.executor_id, self.leader_id,
                    )
                    return False
        return False

    def _execute_order(self, order_id, items):
        """Drive 2PC across payment and books_database for this order.

        Phase 1 (Prepare): ask each participant to vote. Any NO vote or exception → Abort.
        Phase 2 (Commit/Abort): broadcast the decision with bounded retry on transient errors.
        Only Commit causes participants to execute their real side-effects; Prepare only stages.
        """
        amount = _dummy_amount(items)

        pay_ch = grpc.insecure_channel(PAYMENT_ADDR)
        db_ch = grpc.insecure_channel(BOOKS_DB_ADDR)
        try:
            pay = payment_pb2_grpc.PaymentServiceStub(pay_ch)
            db = db_grpc.BooksDatabaseStub(db_ch)

            logging.info(
                "[Executor %d] 2PC %s: Prepare sent (payment=$%d, books_db items=%d)",
                self.executor_id, order_id, amount, len(items),
            )

            votes = {}
            reasons = {}

            # Phase 1 — Prepare.
            try:
                resp = pay.Prepare(
                    payment_pb2.PreparePaymentRequest(order_id=order_id, amount=amount),
                    timeout=PREPARE_TIMEOUT,
                )
                votes["payment"] = bool(resp.ready)
                reasons["payment"] = resp.reason or ("ready" if resp.ready else "not ready")
            except Exception as e:
                logging.warning("[Executor %d] Prepare to payment failed: %s", self.executor_id, e)
                votes["payment"] = False
                reasons["payment"] = f"exception: {e}"

            try:
                resp = db.Prepare(
                    db_pb2.PrepareStockRequest(
                        order_id=order_id,
                        items=[db_pb2.StockItem(title=i.title, quantity=i.quantity) for i in items],
                    ),
                    timeout=PREPARE_TIMEOUT,
                )
                votes["books_db"] = bool(resp.ready)
                reasons["books_db"] = resp.reason or ("ready" if resp.ready else "not ready")
            except Exception as e:
                logging.warning("[Executor %d] Prepare to books_db failed: %s", self.executor_id, e)
                votes["books_db"] = False
                reasons["books_db"] = f"exception: {e}"

            logging.info(
                "[Executor %d] 2PC %s: votes collected: %s",
                self.executor_id, order_id, votes,
            )

            # Phase 2 — Commit or Abort.
            if all(votes.values()):
                logging.info(
                    "[Executor %d] 2PC %s: decision=COMMIT",
                    self.executor_id, order_id,
                )
                self._broadcast_finalize(pay, db, order_id, "Commit")
                logging.info(
                    "[Executor %d] 2PC %s: COMMITTED",
                    self.executor_id, order_id,
                )
                self._notify_orchestrator(order_id, "committed", "")
                return True

            reason = "; ".join(f"{n}: {reasons[n]}" for n, v in votes.items() if not v)
            logging.info(
                "[Executor %d] 2PC %s: decision=ABORT (%s)",
                self.executor_id, order_id, reason,
            )
            self._broadcast_finalize(pay, db, order_id, "Abort")
            logging.info(
                "[Executor %d] 2PC %s: ABORTED",
                self.executor_id, order_id,
            )
            self._notify_orchestrator(order_id, "aborted", reason)
            return False
        finally:
            try:
                pay_ch.close()
            finally:
                db_ch.close()

    def _broadcast_finalize(self, pay, db, order_id, verb):
        """Send Commit/Abort to each participant with bounded retry on transient failures.

        The coordinator has already decided; retries here cover network blips, not the
        2PC coordinator-crash blocking window (that's a separate analysis).
        """
        if verb == "Commit":
            calls = [
                ("payment", lambda: pay.Commit(
                    payment_pb2.CommitRequest(order_id=order_id), timeout=FINALIZE_TIMEOUT)),
                ("books_db", lambda: db.Commit(
                    db_pb2.CommitRequest(order_id=order_id), timeout=FINALIZE_TIMEOUT)),
            ]
        elif verb == "Abort":
            calls = [
                ("payment", lambda: pay.Abort(
                    payment_pb2.AbortRequest(order_id=order_id), timeout=FINALIZE_TIMEOUT)),
                ("books_db", lambda: db.Abort(
                    db_pb2.AbortRequest(order_id=order_id), timeout=FINALIZE_TIMEOUT)),
            ]
        else:
            raise ValueError(f"unknown finalize verb: {verb}")

        for name, call in calls:
            for attempt in range(1, FINALIZE_RETRIES + 1):
                try:
                    call()
                    break
                except Exception as e:
                    logging.warning(
                        "[Executor %d] %s to %s failed (attempt %d/%d): %s",
                        self.executor_id, verb, name, attempt, FINALIZE_RETRIES, e,
                    )
                    if attempt < FINALIZE_RETRIES:
                        time.sleep(FINALIZE_BACKOFF)
            else:
                # Exhausted retries — log and move on. Participant's idempotent handling
                # means a future retry (or journal replay) can still converge.
                logging.error(
                    "[Executor %d] %s to %s exhausted retries — giving up on this run",
                    self.executor_id, verb, name,
                )

    def _notify_orchestrator(self, order_id, outcome, reason):
        """Best-effort POST to orchestrator so the frontend's status poll can resolve."""
        body = json.dumps({"orderId": order_id, "outcome": outcome, "reason": reason}).encode("utf-8")
        req = urllib.request.Request(
            ORCHESTRATOR_OUTCOME_URL, data=body,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        try:
            with urllib.request.urlopen(req, timeout=3):
                pass
        except Exception as e:
            logging.warning(
                "[Executor %d] could not notify orchestrator for %s: %s",
                self.executor_id, order_id, e,
            )

    def run_leader_loop(self):
        """Leader: repeatedly dequeue and execute orders."""
        logging.info("[Executor %d] Running as leader, dequeuing orders...", self.executor_id)
        oq_channel = grpc.insecure_channel(ORDER_QUEUE_ADDR)
        oq_stub = oq_grpc.OrderQueueServiceStub(oq_channel)
        try:
            while self.leader_id == self.executor_id:
                try:
                    response = oq_stub.Dequeue(oq_pb2.DequeueRequest(), timeout=5)
                    if response.found:
                        logging.info(
                            "[Executor %d] Executing order %s (%d items)...",
                            self.executor_id, response.orderId, len(response.items),
                        )
                        self._execute_order(response.orderId, response.items)
                        logging.info(
                            "[Executor %d] Order %s completed",
                            self.executor_id, response.orderId,
                        )
                    else:
                        logging.debug(
                            "[Executor %d] Queue empty, waiting...", self.executor_id,
                        )
                except Exception as e:
                    logging.error(
                        "[Executor %d] Error dequeuing: %s", self.executor_id, e,
                    )
                time.sleep(DEQUEUE_INTERVAL)
        finally:
            oq_channel.close()

    def run_follower_loop(self):
        """Non-leader: periodically check if leader is alive."""
        logging.info(
            "[Executor %d] Running as follower (leader=%s)",
            self.executor_id, self.leader_id,
        )
        while self.leader_id != self.executor_id:
            time.sleep(HEARTBEAT_INTERVAL)
            if not self.check_leader_alive():
                logging.info(
                    "[Executor %d] Leader lost, triggering new election",
                    self.executor_id,
                )
                self.leader_id = None
                self.start_election()
                return  # After election, run() will pick the right loop

    def run(self):
        """Main loop: elect leader, then run as leader or follower."""
        # Brief startup delay to let peers come online
        time.sleep(2)
        self.start_election()

        while True:
            if self.leader_id == self.executor_id:
                self.run_leader_loop()
            else:
                self.run_follower_loop()


def serve():
    node = ExecutorNode()

    # Start gRPC server for incoming Election/Victory RPCs
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    ex_grpc.add_ExecutorServiceServicer_to_server(ExecutorServiceServicer(node), server)
    server.add_insecure_port("[::]:50055")
    logging.info(
        "Executor %d listening on port 50055 (peers: %s, queue: %s)",
        EXECUTOR_ID, PEERS, ORDER_QUEUE_ADDR,
    )
    server.start()

    # Run the election + execution loop in a separate thread
    runner = threading.Thread(target=node.run, daemon=True)
    runner.start()

    server.wait_for_termination()


if __name__ == "__main__":
    serve()
