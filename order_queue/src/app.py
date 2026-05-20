import sys
import os
import threading
import logging
import time
from collections import deque
from concurrent import futures

import grpc

FILE = __file__ if "__file__" in globals() else os.getenv("PYTHONFILE", "")

oq_grpc_path = os.path.abspath(os.path.join(FILE, "../../../utils/pb/order_queue"))
sys.path.insert(0, oq_grpc_path)
import order_queue_pb2 as oq_pb2
import order_queue_pb2_grpc as oq_grpc

project_root = os.path.abspath(os.path.join(FILE, "../../.."))
sys.path.insert(0, project_root)
from utils.observability import configure_otel, record_exception, server_span

logging.basicConfig(level=logging.INFO)

tracer, meter = configure_otel(os.environ.get("OTEL_SERVICE_NAME", "order_queue"))
queue_operations_counter = meter.create_counter(
    "order_queue_operations_total",
    description="Total number of order queue operations",
)
queue_wait_duration = meter.create_histogram(
    "order_queue_wait_seconds",
    description="Time orders spend waiting in the queue",
    unit="s",
)


class OrderQueueServiceServicer(oq_grpc.OrderQueueServiceServicer):
    def __init__(self):
        self._lock = threading.Lock()
        self._queue = deque()  # stores (orderId, [BookItem, ...]) tuples

    def Enqueue(self, request, context):
        with server_span(
            tracer, context, "order_queue.enqueue", **{"order.id": request.orderId}
        ) as span:
            try:
                with self._lock:
                    self._queue.append((request.orderId, list(request.items), time.time()))
                    queue_size = len(self._queue)
                    logging.info("Enqueued order %s (queue size: %d)", request.orderId, queue_size)
                if hasattr(span, "set_attribute"):
                    span.set_attribute("queue.size", queue_size)
                queue_operations_counter.add(1, {"operation": "enqueue", "result": "success"})
                return oq_pb2.EnqueueResponse(success=True)
            except Exception as exc:
                queue_operations_counter.add(1, {"operation": "enqueue", "result": "error"})
                if hasattr(span, "record_exception"):
                    record_exception(span, exc)
                raise

    def Dequeue(self, request, context):
        with server_span(tracer, context, "order_queue.dequeue") as span:
            with self._lock:
                if self._queue:
                    order_id, items, enqueued_at = self._queue.popleft()
                    queue_size = len(self._queue)
                    wait_seconds = time.time() - enqueued_at
                    logging.info("Dequeued order %s (queue size: %d)", order_id, queue_size)
                    queue_wait_duration.record(wait_seconds, {"result": "found"})
                    queue_operations_counter.add(1, {"operation": "dequeue", "result": "found"})
                    if hasattr(span, "set_attribute"):
                        span.set_attribute("order.id", order_id)
                        span.set_attribute("queue.size", queue_size)
                        span.set_attribute("queue.wait_seconds", wait_seconds)
                    return oq_pb2.DequeueResponse(orderId=order_id, found=True, items=items)
                queue_operations_counter.add(1, {"operation": "dequeue", "result": "empty"})
                return oq_pb2.DequeueResponse(orderId="", found=False)


def serve():
    servicer = OrderQueueServiceServicer()

    def _queue_depth_callback(_options):
        with servicer._lock:
            yield metrics.Observation(len(servicer._queue), {})

    from opentelemetry import metrics
    meter.create_observable_gauge(
        "order_queue_depth",
        callbacks=[_queue_depth_callback],
        description="Current number of orders waiting in the queue",
    )

    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    oq_grpc.add_OrderQueueServiceServicer_to_server(servicer, server)
    server.add_insecure_port("[::]:50054")
    logging.info("OrderQueue service listening on port 50054")
    server.start()
    server.wait_for_termination()


if __name__ == "__main__":
    serve()
