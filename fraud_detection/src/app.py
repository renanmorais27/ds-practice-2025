import sys
import os

# This set of lines are needed to import the gRPC stubs.
# The path of the stubs is relative to the current file, or absolute inside the container.
# Change these lines only if strictly needed.
FILE = __file__ if "__file__" in globals() else os.getenv("PYTHONFILE", "")
fd_grpc_path = os.path.abspath(os.path.join(FILE, "../../../utils/pb/fraud_detection"))
sys.path.insert(0, fd_grpc_path)
import fraud_detection_pb2 as fd_pb2
import fraud_detection_pb2_grpc as fd_grpc

import logging
import grpc
from concurrent import futures

logging.basicConfig(level=logging.INFO)


class FraudDetectionService(fd_grpc.FraudDetectionServiceServicer):

    def CheckFraud(self, request, context):
        """Check for fraud based on two rules: order amount exceeding 1000 and card number prefix '999'."""
        card_number = request.card_number
        order_amount = request.order_amount

        logging.info(
            f"Checking fraud for card ending in {card_number[-4:]} with amount {order_amount}"
        )

        is_fraud = False
        if float(order_amount) > 1000 or card_number.startswith("999"):
            is_fraud = True

        logging.info(f"Fraud check result: is_fraud={is_fraud}")
        return fd_pb2.FraudResponse(is_fraud=is_fraud)


def serve():
    server = grpc.server(futures.ThreadPoolExecutor())
    fd_grpc.add_FraudDetectionServiceServicer_to_server(FraudDetectionService(), server)
    port = "50051"
    server.add_insecure_port("[::]:" + port)
    server.start()
    logging.info(f"Fraud Detection service started on port {port}")
    server.wait_for_termination()


if __name__ == "__main__":
    serve()
