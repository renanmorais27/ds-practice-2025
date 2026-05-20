import sys
import os
import json
import threading
import logging
import time
from concurrent import futures

import grpc

from opentelemetry import trace, metrics
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
from opentelemetry.exporter.otlp.proto.http.metric_exporter import OTLPMetricExporter
from opentelemetry.sdk.resources import Resource

FILE = __file__ if "__file__" in globals() else os.getenv("PYTHONFILE", "")

db_grpc_path = os.path.abspath(os.path.join(FILE, "../../../utils/pb/books_database"))
sys.path.insert(0, db_grpc_path)
import books_database_pb2 as db_pb2
import books_database_pb2_grpc as db_grpc

logging.basicConfig(level=logging.INFO)

REPLICA_ROLE = os.environ.get("REPLICA_ROLE", "backup")  # "primary" or "backup"
BACKUP_ADDRS = os.environ.get("BACKUP_ADDRS", "")        # comma-separated, primary only
JOURNAL_PATH = os.environ.get("BOOKS_DB_JOURNAL", "")     # if set, persist _tx across restarts

_otel_endpoint = os.environ.get("OTEL_EXPORTER_OTLP_ENDPOINT", "http://observability:4318")
_service_name = os.environ.get("OTEL_SERVICE_NAME", "books_database")
_resource = Resource.create({"service.name": _service_name})

_tracer_provider = TracerProvider(resource=_resource)
_tracer_provider.add_span_processor(
    BatchSpanProcessor(OTLPSpanExporter(endpoint=f"{_otel_endpoint}/v1/traces"))
)
trace.set_tracer_provider(_tracer_provider)

_meter_provider = MeterProvider(
    resource=_resource,
    metric_readers=[PeriodicExportingMetricReader(
        OTLPMetricExporter(endpoint=f"{_otel_endpoint}/v1/metrics"),
        export_interval_millis=5000,
    )],
)
metrics.set_meter_provider(_meter_provider)

tracer = trace.get_tracer(_service_name)
meter = metrics.get_meter(_service_name)

db_operations_counter = meter.create_counter(
    "db_operations_total",
    description="Total number of database operations (prepare/commit/abort/read/write)",
)
staged_tx_counter = meter.create_up_down_counter(
    "staged_transactions",
    description="Number of transactions currently in STAGED state",
)
operation_duration = meter.create_histogram(
    "db_operation_duration_seconds",
    description="Duration of database operations in seconds",
    unit="s",
)

INITIAL_STOCK = {
    "Distributed systems.": 5000,
    "Introduction to gRPC.": 5000,
}

STAGED, COMMITTED, ABORTED = "staged", "committed", "aborted"


class BooksDatabaseServicer(db_grpc.BooksDatabaseServicer):
    def __init__(self):
        self._lock = threading.Lock()
        self.store = dict(INITIAL_STOCK)

        # order_id -> (state, [(title, qty), ...])
        # committed/aborted entries linger as tombstones so Commit/Abort retries are no-ops.
        self._tx = {}

        # Journal replay — reapply any COMMITTED transactions we had already decided on
        # before the last restart, then restore the full _tx map so Prepare/Commit/Abort
        # idempotency works across restarts. Only Prepare votes that reached STAGED are
        # kept; a crash before STAGED means we never voted YES, so nothing is owed.
        if JOURNAL_PATH:
            self._load_journal()

    def _load_journal(self):
        try:
            with open(JOURNAL_PATH, "r") as f:
                raw = json.load(f)
        except FileNotFoundError:
            return
        except Exception as e:
            logging.warning("journal load failed: %s", e)
            return

        stored_tx = raw.get("tx", {})
        stored_store = raw.get("store", {})
        if stored_store:
            self.store = dict(stored_store)

        for order_id, entry in stored_tx.items():
            state = entry.get("state")
            items = [(i["title"], i["quantity"]) for i in entry.get("items", [])]
            self._tx[order_id] = (state, items)
        logging.info("journal: reloaded %d tx entries, store=%s", len(self._tx), self.store)

    def _persist_journal_locked(self):
        """Caller must hold self._lock. Atomic write via rename."""
        if not JOURNAL_PATH:
            return
        payload = {
            "store": self.store,
            "tx": {
                oid: {
                    "state": state,
                    "items": [{"title": t, "quantity": q} for t, q in items],
                }
                for oid, (state, items) in self._tx.items()
            },
        }
        tmp = JOURNAL_PATH + ".tmp"
        try:
            with open(tmp, "w") as f:
                json.dump(payload, f)
            os.replace(tmp, JOURNAL_PATH)
        except Exception as e:
            logging.warning("journal write failed: %s", e)

    def Read(self, request, context):
        _t0 = time.time()
        with self._lock:
            stock = self.store.get(request.title, 0)
        db_operations_counter.add(1, {"operation": "read", "role": REPLICA_ROLE})
        operation_duration.record(time.time() - _t0, {"operation": "read"})
        logging.info("Read '%s' -> stock=%d", request.title, stock)
        return db_pb2.ReadResponse(stock=stock)

    def Write(self, request, context):
        with self._lock:
            self.store[request.title] = request.new_stock
        logging.info("Write '%s' = %d", request.title, request.new_stock)
        return db_pb2.WriteResponse(success=True)

    def Increment(self, request, context):
        with self._lock:
            current = self.store.get(request.title, 0)
            self.store[request.title] = current + request.quantity
        logging.info("Increment '%s': %d -> %d", request.title, current, current + request.quantity)
        return db_pb2.IncrementResponse(success=True)

    # ── 2PC participant ──

    def Prepare(self, request, context):
        _t0 = time.time()
        with tracer.start_as_current_span("db.prepare") as span:
            span.set_attribute("order.id", request.order_id)
            span.set_attribute("db.items_count", len(request.items))

            for item in request.items:
                if item.quantity <= 0:
                    db_operations_counter.add(1, {"operation": "prepare", "result": "invalid"})
                    operation_duration.record(time.time() - _t0, {"operation": "prepare"})
                    return db_pb2.PrepareStockResponse(
                        ready=False, reason=f"invalid quantity for '{item.title}'",
                    )

            with self._lock:
                prior = self._tx.get(request.order_id)
                if prior is not None:
                    state, _ = prior
                    db_operations_counter.add(1, {"operation": "prepare", "result": "idempotent"})
                    operation_duration.record(time.time() - _t0, {"operation": "prepare"})
                    return db_pb2.PrepareStockResponse(ready=(state == STAGED))

                for item in request.items:
                    if self.store.get(item.title, 0) < item.quantity:
                        self._tx[request.order_id] = (ABORTED, [])
                        self._persist_journal_locked()
                        logging.info(
                            "Prepare %s: insufficient '%s' (have %d, need %d)",
                            request.order_id, item.title,
                            self.store.get(item.title, 0), item.quantity,
                        )
                        db_operations_counter.add(1, {"operation": "prepare", "result": "insufficient_stock"})
                        operation_duration.record(time.time() - _t0, {"operation": "prepare"})
                        return db_pb2.PrepareStockResponse(
                            ready=False, reason=f"insufficient '{item.title}'",
                        )

                staged = [(i.title, i.quantity) for i in request.items]
                self._tx[request.order_id] = (STAGED, staged)
                self._persist_journal_locked()

            staged_tx_counter.add(1, {"role": REPLICA_ROLE})
            db_operations_counter.add(1, {"operation": "prepare", "result": "staged"})
            operation_duration.record(time.time() - _t0, {"operation": "prepare"})
            logging.info("Prepare %s: STAGED items=%s", request.order_id, staged)
            return db_pb2.PrepareStockResponse(ready=True)

    def Commit(self, request, context):
        _t0 = time.time()
        with tracer.start_as_current_span("db.commit") as span:
            span.set_attribute("order.id", request.order_id)

            replicate_values = None
            with self._lock:
                entry = self._tx.get(request.order_id)
                if entry is None or entry[0] != STAGED:
                    logging.info("Commit %s: no-op (entry=%s)", request.order_id, entry)
                    db_operations_counter.add(1, {"operation": "commit", "result": "noop"})
                    operation_duration.record(time.time() - _t0, {"operation": "commit"})
                    return db_pb2.CommitResponse(success=True)
                _, staged = entry
                for title, qty in staged:
                    self.store[title] = self.store.get(title, 0) - qty
                self._tx[request.order_id] = (COMMITTED, staged)
                self._persist_journal_locked()
                replicate_values = {t: self.store[t] for t, _ in staged}

            staged_tx_counter.add(-1, {"role": REPLICA_ROLE})
            db_operations_counter.add(1, {"operation": "commit", "result": "committed"})
            operation_duration.record(time.time() - _t0, {"operation": "commit"})
            logging.info("Commit %s: decremented store=%s", request.order_id, replicate_values)
            self._after_commit(replicate_values)
            return db_pb2.CommitResponse(success=True)

    def Abort(self, request, context):
        with self._lock:
            entry = self._tx.get(request.order_id)
            was_staged = entry is not None and entry[0] == STAGED
            if entry is None:
                self._tx[request.order_id] = (ABORTED, [])
                self._persist_journal_locked()
            elif entry[0] == STAGED:
                self._tx[request.order_id] = (ABORTED, [])
                self._persist_journal_locked()

        if was_staged:
            staged_tx_counter.add(-1, {"role": REPLICA_ROLE})
        db_operations_counter.add(1, {"operation": "abort", "role": REPLICA_ROLE})
        logging.info("Abort %s", request.order_id)
        return db_pb2.AbortResponse(aborted=True)

    def _after_commit(self, replicate_values):
        """Hook for primary-to-backup replication. No-op on a pure backup."""
        return


class PrimaryReplica(BooksDatabaseServicer):
    """Primary replica: handles all writes and propagates them to backup replicas."""

    def __init__(self, backup_addrs):
        super().__init__()
        self._backup_addrs = [a.strip() for a in backup_addrs if a.strip()]

    def _replicate(self, method_name, request):
        for addr in self._backup_addrs:
            try:
                with grpc.insecure_channel(addr) as channel:
                    stub = db_grpc.BooksDatabaseStub(channel)
                    getattr(stub, method_name)(request, timeout=3)
            except Exception as e:
                logging.warning("Failed to replicate %s to backup %s: %s", method_name, addr, e)

    def Write(self, request, context):
        with self._lock:
            self.store[request.title] = request.new_stock
        logging.info("Write '%s' = %d (primary)", request.title, request.new_stock)
        self._replicate("Write", request)
        return db_pb2.WriteResponse(success=True)

    def Increment(self, request, context):
        with self._lock:
            current = self.store.get(request.title, 0)
            self.store[request.title] = current + request.quantity
        logging.info(
            "Increment '%s': %d -> %d (primary)",
            request.title, current, current + request.quantity,
        )
        self._replicate("Increment", request)
        return db_pb2.IncrementResponse(success=True)

    def _after_commit(self, replicate_values):
        # Replication runs OUTSIDE self._lock so a flaky backup can't stall Reads or
        # the next Prepare. We ship absolute values via Write rather than deltas so a
        # backup receiving out-of-order messages still converges to the latest value —
        # acceptable because only a single elected executor leader drives 2PC at a time.
        if not replicate_values:
            return
        for title, new_stock in replicate_values.items():
            self._replicate("Write", db_pb2.WriteRequest(title=title, new_stock=new_stock))


def serve():
    backup_addrs = [a for a in BACKUP_ADDRS.split(",") if a.strip()] if BACKUP_ADDRS else []

    if REPLICA_ROLE == "primary":
        servicer = PrimaryReplica(backup_addrs)
        logging.info("Starting as PRIMARY, backups: %s", backup_addrs)
    else:
        servicer = BooksDatabaseServicer()
        logging.info("Starting as BACKUP")

    def _stock_gauge_callback(_options):
        with servicer._lock:
            for title, stock in servicer.store.items():
                yield metrics.Observation(stock, {"book": title, "role": REPLICA_ROLE})

    meter.create_observable_gauge(
        "book_stock_level",
        callbacks=[_stock_gauge_callback],
        description="Current stock level per book title",
    )

    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    db_grpc.add_BooksDatabaseServicer_to_server(servicer, server)
    server.add_insecure_port("[::]:50060")
    logging.info("BooksDatabase listening on port 50060 (role=%s)", REPLICA_ROLE)
    server.start()
    server.wait_for_termination()


if __name__ == "__main__":
    serve()
