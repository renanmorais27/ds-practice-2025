import sys
import os

# This set of lines are needed to import the gRPC stubs.
# The path of the stubs is relative to the current file, or absolute inside the container.
# Change these lines only if strictly needed.
FILE = __file__ if "__file__" in globals() else os.getenv("PYTHONFILE", "")
sg_grpc_path = os.path.abspath(os.path.join(FILE, "../../../utils/pb/suggestions"))
sys.path.insert(0, sg_grpc_path)
import suggestions_pb2 as sg_pb2
import suggestions_pb2_grpc as sg_grpc

import logging
import grpc
from concurrent import futures

logging.basicConfig(level=logging.INFO)


class SuggestionsService(sg_grpc.SuggestionsServiceServicer):

    def GetSuggestions(self, request, context):
        """Return a static list of book suggestions (not yet context-aware)."""
        logging.info(f"Getting suggestions for {len(request.items)} items")

        # Static list of suggested books
        suggested_books = [
            sg_pb2.SuggestedBook(
                bookId="123", title="The Best Book", author="Author 1"
            ),
            sg_pb2.SuggestedBook(
                bookId="456", title="The Second Best Book", author="Author 2"
            ),
        ]

        logging.info(f"Returning {len(suggested_books)} suggestions")
        return sg_pb2.SuggestionsResponse(books=suggested_books)


def serve():
    server = grpc.server(futures.ThreadPoolExecutor())
    sg_grpc.add_SuggestionsServiceServicer_to_server(SuggestionsService(), server)
    port = "50053"
    server.add_insecure_port("[::]:" + port)
    server.start()
    logging.info(f"Suggestions service started on port {port}")
    server.wait_for_termination()


if __name__ == "__main__":
    serve()
