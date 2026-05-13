"""
Scenario 2: Multiple non-fraudulent non-conflicting orders.

Each simulated user orders a DIFFERENT book (round-robin across available books),
so no two concurrent orders compete for the same stock.
All orders are expected to reach state "committed".

System capacity (measured):
    - Ideal: 3 concurrent users
    - Max supportable: 5-6 concurrent users
    - Beyond 6: orders time out before the executor can process them

Run:
    locust -f scenario2.py --host http://localhost:8081 --users 3 --spawn-rate 1 --run-time 60s --headless
    or
    locust -f /Users/leopoldpichonneau/ds-practice-2025/tests/locust/scenario2.py --host http://localhost:8081
"""

import itertools
import threading
import time

from locust import HttpUser, between, task

BOOKS = ["Distributed systems.", "Introduction to gRPC."]

_counter = itertools.count()
_lock = threading.Lock()


def _next_book():
    with _lock:
        return BOOKS[next(_counter) % len(BOOKS)]


BASE_PAYLOAD = {
    "user": {"name": "John Doe", "contact": "john.doe@example.com"},
    "creditCard": {
        "number": "4111111111111111",
        "expirationDate": "12/28",
        "cvv": "123",
    },
    "billingAddress": {
        "street": "123 Main St",
        "city": "Springfield",
        "state": "IL",
        "zip": "62701",
        "country": "USA",
    },
    "shippingMethod": "Standard",
    "giftWrapping": True,
    "termsAccepted": True,
}


class NonConflictingUser(HttpUser):
    wait_time = between(1, 3)  # stagger arrivals to simulate real users

    @task
    def place_order(self):
        book = _next_book()
        payload = {**BASE_PAYLOAD, "items": [{"name": book, "quantity": 1}]}

        with self.client.post("/checkout", json=payload, catch_response=True) as r:
            if not r.ok:
                r.failure(f"HTTP {r.status_code}")
                return
            body = r.json()
            if body.get("status") == "Order Denied":
                r.failure(f"Denied: {body.get('reason')}")
                return
            order_id = body.get("orderId")
            if not order_id:
                r.failure("Missing orderId in checkout response")
                return

        self._poll(order_id)

    def _poll(self, order_id, timeout=30):
        deadline = time.time() + timeout
        while time.time() < deadline:
            time.sleep(1.5)
            with self.client.get(
                f"/order-status/{order_id}",
                name="/order-status/[id]",
                catch_response=True,
            ) as r:
                if not r.ok:
                    r.failure(f"Poll HTTP {r.status_code}")
                    return
                state = r.json().get("state")
                if state == "committed":
                    r.success()
                    return
                if state == "aborted":
                    r.failure(f"Order aborted: {r.json().get('reason', '')}")
                    return
                r.success()  # still pending, keep polling
