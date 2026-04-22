import sys
import os
import threading
import logging
from concurrent import futures

import grpc

FILE = __file__ if "__file__" in globals() else os.getenv("PYTHONFILE", "")

db_grpc_path = os.path.abspath(os.path.join(FILE, "../../../utils/pb/books_database"))
sys.path.insert(0, db_grpc_path)
import books_database_pb2 as db_pb2
import books_database_pb2_grpc as db_grpc

logging.basicConfig(level=logging.INFO)

REPLICA_ROLE = os.environ.get("REPLICA_ROLE", "backup")  # "primary" or "backup"
BACKUP_ADDRS = os.environ.get("BACKUP_ADDRS", "")  # comma-separated, primary only

INITIAL_STOCK = {
    "Distributed systems.": 3,
    "Introduction to gRPC.": 5,
}


class BooksDatabaseServicer(db_grpc.BooksDatabaseServicer):
    def __init__(self):
        self._lock = threading.Lock()
        self.store = dict(INITIAL_STOCK)

    def Read(self, request, context):
        with self._lock:
            stock = self.store.get(request.title, 0)
        logging.info("Read '%s' -> stock=%d", request.title, stock)
        return db_pb2.ReadResponse(stock=stock)

    def Write(self, request, context):
        with self._lock:
            self.store[request.title] = request.new_stock
        logging.info("Write '%s' = %d", request.title, request.new_stock)
        return db_pb2.WriteResponse(success=True)

    def TryDecrement(self, request, context):
        # Atomic check-and-reserve: the stock check and the decrement happen under the same lock,
        # so concurrent callers cannot both observe "enough stock" and then both subtract.
        # Returns success=False (without mutating) if stock is insufficient; caller must retry or abort.
        with self._lock:
            current = self.store.get(request.title, 0)
            if current < request.quantity:
                logging.info("TryDecrement '%s': insufficient (have %d, need %d)", request.title, current, request.quantity)
                return db_pb2.TryDecrementResponse(success=False)
            self.store[request.title] = current - request.quantity
        logging.info("TryDecrement '%s': %d -> %d", request.title, current, current - request.quantity)
        return db_pb2.TryDecrementResponse(success=True)

    def Increment(self, request, context):
        with self._lock:
            current = self.store.get(request.title, 0)
            self.store[request.title] = current + request.quantity
        logging.info("Increment '%s': %d -> %d", request.title, current, current + request.quantity)
        return db_pb2.IncrementResponse(success=True)


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

    def TryDecrement(self, request, context):
        # Atomic check-and-reserve on the primary, then replicate to backups.
        # The check and the decrement share one critical section, so the primary is the single
        # serialization point for stock reservation — two concurrent requests cannot both succeed
        # when only one's worth of stock exists. Backups receive the same decrement only if the
        # primary committed, keeping replicas consistent with the reservation decision.
        with self._lock:
            current = self.store.get(request.title, 0)
            if current < request.quantity:
                logging.info("TryDecrement '%s': insufficient (have %d, need %d) (primary)", request.title, current, request.quantity)
                return db_pb2.TryDecrementResponse(success=False)
            self.store[request.title] = current - request.quantity
        logging.info("TryDecrement '%s': %d -> %d (primary)", request.title, current, current - request.quantity)
        self._replicate("TryDecrement", request)
        return db_pb2.TryDecrementResponse(success=True)

    def Increment(self, request, context):
        with self._lock:
            current = self.store.get(request.title, 0)
            self.store[request.title] = current + request.quantity
        logging.info("Increment '%s': %d -> %d (primary)", request.title, current, current + request.quantity)
        self._replicate("Increment", request)
        return db_pb2.IncrementResponse(success=True)


def serve():
    backup_addrs = [a for a in BACKUP_ADDRS.split(",") if a.strip()] if BACKUP_ADDRS else []

    if REPLICA_ROLE == "primary":
        servicer = PrimaryReplica(backup_addrs)
        logging.info("Starting as PRIMARY, backups: %s", backup_addrs)
    else:
        servicer = BooksDatabaseServicer()
        logging.info("Starting as BACKUP")

    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    db_grpc.add_BooksDatabaseServicer_to_server(servicer, server)
    server.add_insecure_port("[::]:50060")
    logging.info("BooksDatabase listening on port 50060 (role=%s)", REPLICA_ROLE)
    server.start()
    server.wait_for_termination()


if __name__ == "__main__":
    serve()
