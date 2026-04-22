import sys
import os
import threading
import logging
from collections import deque
from concurrent import futures

import grpc

FILE = __file__ if "__file__" in globals() else os.getenv("PYTHONFILE", "")

oq_grpc_path = os.path.abspath(os.path.join(FILE, "../../../utils/pb/order_queue"))
sys.path.insert(0, oq_grpc_path)
import order_queue_pb2 as oq_pb2
import order_queue_pb2_grpc as oq_grpc

logging.basicConfig(level=logging.INFO)


class OrderQueueServiceServicer(oq_grpc.OrderQueueServiceServicer):
    def __init__(self):
        self._lock = threading.Lock()
        self._queue = deque()  # stores (orderId, [BookItem, ...]) tuples

    def Enqueue(self, request, context):
        with self._lock:
            self._queue.append((request.orderId, list(request.items)))
            logging.info("Enqueued order %s (queue size: %d)", request.orderId, len(self._queue))
        return oq_pb2.EnqueueResponse(success=True)

    def Dequeue(self, request, context):
        with self._lock:
            if self._queue:
                order_id, items = self._queue.popleft()
                logging.info("Dequeued order %s (queue size: %d)", order_id, len(self._queue))
                return oq_pb2.DequeueResponse(orderId=order_id, found=True, items=items)
            return oq_pb2.DequeueResponse(orderId="", found=False)


def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    oq_grpc.add_OrderQueueServiceServicer_to_server(OrderQueueServiceServicer(), server)
    server.add_insecure_port("[::]:50054")
    logging.info("OrderQueue service listening on port 50054")
    server.start()
    server.wait_for_termination()


if __name__ == "__main__":
    serve()
