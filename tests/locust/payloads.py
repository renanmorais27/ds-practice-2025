import itertools
import threading

BOOKS = ["Distributed systems.", "Introduction to gRPC."]

_counter = itertools.count()
_lock = threading.Lock()


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


def _payload_with_items(items, **overrides):
    payload = {
        **BASE_PAYLOAD,
        "user": dict(BASE_PAYLOAD["user"]),
        "creditCard": dict(BASE_PAYLOAD["creditCard"]),
        "billingAddress": dict(BASE_PAYLOAD["billingAddress"]),
        "items": items,
    }
    for key, value in overrides.items():
        if isinstance(value, dict) and isinstance(payload.get(key), dict):
            payload[key].update(value)
        else:
            payload[key] = value
    return payload


def next_non_conflicting_payload():
    with _lock:
        book = BOOKS[next(_counter) % len(BOOKS)]
    return _payload_with_items([{"name": book, "quantity": 1}])


def single_valid_payload():
    return _payload_with_items([{"name": "Distributed systems.", "quantity": 1}])


def fraudulent_payload():
    return _payload_with_items(
        [{"name": "Introduction to gRPC.", "quantity": 1}],
        user={"name": "Fraud Tester", "contact": "fraud@example.com"},
        creditCard={"number": "9991111111111111"},
    )


def invalid_payload():
    return _payload_with_items(
        [{"name": "Distributed systems.", "quantity": 1}],
        creditCard={"number": "1234"},
    )


def conflict_payload():
    return _payload_with_items([{"name": "Distributed systems.", "quantity": 1}])
