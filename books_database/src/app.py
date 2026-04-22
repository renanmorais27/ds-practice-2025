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


class PrimaryReplica(BooksDatabaseServicer):
    """Primary replica: handles all writes and propagates them to backup replicas."""

    def __init__(self, backup_addrs):
        super().__init__()
        self._backup_addrs = [a.strip() for a in backup_addrs if a.strip()]

    def Write(self, request, context):
        # Write locally first
        with self._lock:
            self.store[request.title] = request.new_stock
        logging.info("Write '%s' = %d (primary)", request.title, request.new_stock)

        # Propagate to backups
        for addr in self._backup_addrs:
            try:
                with grpc.insecure_channel(addr) as channel:
                    stub = db_grpc.BooksDatabaseStub(channel)
                    stub.Write(request, timeout=3)
            except Exception as e:
                logging.warning("Failed to replicate to backup %s: %s", addr, e)

        return db_pb2.WriteResponse(success=True)


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
