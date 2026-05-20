import itertools
import os

from locust import HttpUser, between, task

from common import mark_user_started, mark_user_stopped, place_order_and_wait
from payloads import (
    conflict_payload,
    fraudulent_payload,
    invalid_payload,
    next_non_conflicting_payload,
    single_valid_payload,
)

SCENARIO = os.environ.get("LOCUST_SCENARIO", "happy_non_conflicting")
_mixed_counter = itertools.count()


class BookstoreE2EUser(HttpUser):
    wait_time = between(1, 3)

    def on_start(self):
        mark_user_started(SCENARIO)

    def on_stop(self):
        mark_user_stopped(SCENARIO)

    @task
    def run_selected_scenario(self):
        if SCENARIO == "single_non_fraudulent_order":
            place_order_and_wait(self, single_valid_payload(), SCENARIO, "committed")
        elif SCENARIO == "validation_denied":
            place_order_and_wait(self, invalid_payload(), SCENARIO, "denied")
        elif SCENARIO == "fraud_denied":
            place_order_and_wait(self, fraudulent_payload(), SCENARIO, "denied")
        elif SCENARIO == "mixed_orders_simultaneous":
            if next(_mixed_counter) % 2 == 0:
                place_order_and_wait(self, single_valid_payload(), SCENARIO, "committed")
            else:
                place_order_and_wait(self, fraudulent_payload(), SCENARIO, "denied")
        elif SCENARIO == "payment_abort":
            place_order_and_wait(self, single_valid_payload(), SCENARIO, "aborted")
        elif SCENARIO == "same_book_conflict_low_stock":
            place_order_and_wait(self, conflict_payload(), SCENARIO, ("committed", "aborted"))
        elif SCENARIO == "executor_backlog":
            place_order_and_wait(self, next_non_conflicting_payload(), SCENARIO, "committed", timeout=30)
        elif SCENARIO == "suggestions_degraded":
            place_order_and_wait(self, single_valid_payload(), SCENARIO, "committed")
        else:
            place_order_and_wait(self, next_non_conflicting_payload(), "happy_non_conflicting", "committed")
