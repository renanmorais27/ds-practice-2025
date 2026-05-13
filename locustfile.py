"""
Locust load testing configuration for the bookstore API.

Run with: locust -f locustfile.py --host=http://localhost:8081
Or for web UI: locust -f locustfile.py --host=http://localhost:8081 --web
"""

from locust import HttpUser, task, between
import json
import random
import time


class CheckoutUser(HttpUser):
    """Simulates a user performing checkout operations."""

    wait_time = between(1, 3)  # Wait 1-3 seconds between requests

    def on_start(self):
        """Called when a user starts. Can be used for setup."""
        pass

    @task
    def checkout(self):
        """Perform a single checkout request."""
        # Sample checkout payload
        payload = {
            "user": {"name": "Test User", "contact": "test@example.com"},
            "creditCard": {
                "number": "4111111111111111",
                "expirationDate": "12/23",
                "cvv": "123",
            },
            "billingAddress": {
                "street": "123 Main St",
                "city": "New York",
                "state": "NY",
                "zip": "10001",
                "country": "USA",
            },
            "items": [
                # {
                #     "name": f"Book {random.randint(1, 100)}",
                #     "quantity": random.randint(1, 5)
                # }
                {"name": "Distributed systems.", "quantity": 1},
                {"name": "Introduction to gRPC.", "quantity": 1},
            ],
        }

        # Make the POST request
        with self.client.post(
            "/checkout", json=payload, catch_response=True
        ) as response:
            print(f"Checkout response: {response.status_code}")
            response_data = response.json()
            response_data.pop("eventTrace", None)
            response_data.pop("vectorClock", None)
            print(f"Response data: {json.dumps(response_data, indent=2)}")
            if response.status_code == 200:
                # Orchestrator returns status 'accepted' when the order is enqueued
                if response_data.get("status") == "accepted":
                    order_id = response_data.get("orderId")
                    if not order_id:
                        response.failure("No orderId returned")
                        return

                    # Poll /order-status/<order_id> until outcome appears
                    poll_interval = 1.0
                    timeout = 30.0
                    deadline = time.time() + timeout
                    final_state = None
                    reason = None
                    while time.time() < deadline:
                        poll_resp = self.client.get(f"/order-status/{order_id}")
                        if poll_resp.status_code == 200:
                            reason = poll_resp.json().get(
                                "reason", "No reason provided"
                            )
                            state = None
                            try:
                                state = poll_resp.json().get("state")
                            except Exception:
                                state = None
                            if state in ("committed", "aborted"):
                                final_state = state
                                break
                        time.sleep(poll_interval)

                    if final_state == "committed":
                        response.success()
                    elif final_state == "aborted":
                        response.failure(f"Order aborted: {reason}")
                    else:
                        response.failure("Order status polling timed out")
                else:
                    response.failure(
                        f"{response_data.get('reason', 'No reason provided')}"
                    )
            else:
                response.failure(f"Unexpected status code: {response.status_code}")
