---
title: "feat: Add OrderQueue and Executor microservices"
type: feat
status: active
date: 2026-03-25
---

# Add OrderQueue and Executor Microservices

Add two gRPC services: a thread-safe OrderQueue for approved orders and a replicated Executor service (N >= 2) with bully-algorithm leader election for mutual exclusion over order dequeuing.

## Acceptance Criteria

- [x] OrderQueue service with gRPC `Enqueue` and `Dequeue` RPCs, thread-safe with lock
- [x] Orchestrator enqueues approved orders after all 3 services pass, waits for enqueue confirmation, then returns approval to frontend
- [x] Executor service replicated 3 times via explicit docker-compose services
- [x] Leader election using bully algorithm — elected leader has exclusive access to dequeue
- [x] Leader dequeues from OrderQueue and logs "Order {orderId} is being executed..."
- [x] Non-leader replicas wait/watch; re-trigger election if leader becomes unreachable
- [x] Vector clock `SERVICES` tuple updated to include new services

## Architecture Decision: Bully Algorithm

**Choice**: Bully algorithm (highest active ID wins leadership).

**Why**: Simple to implement, deterministic outcome, and fits our setup where each replica has a unique ID. The bully algorithm guarantees that the process with the highest ID among active processes becomes the leader, providing clear mutual exclusion for queue access.

**Trade-offs vs. alternatives**:
- Ring algorithm: more message-efficient but more complex topology management
- Raft/Paxos: overkill for this use case, designed for consensus not simple leader election

**Note**: This architectural choice will be discussed in the next Checkpoint.

## Implementation Steps

### Step 1: Proto definitions

Create `utils/pb/order_queue/order_queue.proto`:

```protobuf
syntax = "proto3";
package order_queue;

service OrderQueueService {
  rpc Enqueue(EnqueueRequest) returns (EnqueueResponse);
  rpc Dequeue(DequeueRequest) returns (DequeueResponse);
}

message EnqueueRequest {
  string orderId = 1;
}

message EnqueueResponse {
  bool success = 1;
}

message DequeueRequest {}

message DequeueResponse {
  string orderId = 1;
  bool found = 2;
}
```

Create `utils/pb/executor/executor.proto`:

```protobuf
syntax = "proto3";
package executor;

service ExecutorService {
  rpc Election(ElectionRequest) returns (ElectionResponse);
  rpc Victory(VictoryRequest) returns (VictoryResponse);
}

message ElectionRequest {
  int32 candidateId = 1;
}

message ElectionResponse {
  bool alive = 1;
}

message VictoryRequest {
  int32 leaderId = 1;
}

message VictoryResponse {
  bool acknowledged = 1;
}
```

Create empty `__init__.py` in both proto dirs.

### Step 2: OrderQueue service

Create `order_queue/` directory following existing pattern:
- `order_queue/Dockerfile` — based on `fraud_detection/Dockerfile`, compile `order_queue.proto` in CMD
- `order_queue/requirements.txt` — baseline gRPC deps (grpcio, grpcio-tools, protobuf, watchdog)
- `order_queue/src/app.py`:
  - `OrderQueueServiceServicer` with `threading.Lock` guarding a `list`
  - `Enqueue`: acquire lock, append orderId to queue, release lock, return `EnqueueResponse(success=True)` — this confirmation is what the orchestrator waits for
  - `Dequeue`: acquire lock, pop(0) if non-empty, release lock, return `DequeueResponse(orderId=id, found=True)` or `DequeueResponse(found=False)`
  - Serve on port `50054`

### Step 3: Executor service

Create `executor/` directory:
- `executor/Dockerfile` — compile both `executor.proto` and `order_queue.proto` (needs queue stub as client)
- `executor/requirements.txt` — baseline gRPC deps
- `executor/src/app.py`:
  - Read `EXECUTOR_ID` and peer addresses from environment variables
  - Start gRPC server to handle incoming `Election` and `Victory` RPCs
  - **Bully election flow**:
    1. On startup (or when leader is unreachable), send `Election(candidateId=self)` to all replicas with higher IDs
    2. If any higher-ID replica responds `alive=True` → stand down, wait for `Victory` message
    3. If no higher-ID replica responds (timeout) → self is leader, broadcast `Victory(leaderId=self)` to all replicas
    4. On receiving `Victory` → update `leader_id`, stand down
    5. On receiving `Election` from lower ID → respond `alive=True`, start own election if not already electing
  - **Leader behavior (mutual exclusion)**: Only the elected leader calls `Dequeue` on OrderQueue. This ensures only one replica accesses a given order at a time. On successful dequeue, log `"Order {orderId} is being executed..."`
  - **Non-leader behavior**: Sleep/poll, periodically check if leader is reachable (heartbeat or Election probe). If leader unreachable → re-trigger election
  - gRPC server on port `50055` (all replicas use the same internal port; docker maps externally)

### Step 4: Docker-compose additions

Use `deploy.replicas` to replicate executors instead of manually defining each instance:

```yaml
order_queue:
  build:
    context: ./
    dockerfile: ./order_queue/Dockerfile
  ports:
    - "50054:50054"
  environment:
    - PYTHONUNBUFFERED=TRUE
  volumes:
    - ./utils:/app/utils
    - ./order_queue/src:/app/order_queue/src

executor:
  build:
    context: ./
    dockerfile: ./executor/Dockerfile
  environment:
    - PYTHONUNBUFFERED=TRUE
    - ORDER_QUEUE_ADDR=order_queue:50054
  volumes:
    - ./utils:/app/utils
    - ./executor/src:/app/executor/src
  deploy:
    replicas: 3
```

**Replica identity**: Each executor needs a unique ID for bully election. Options:
- Use container hostname (docker assigns unique hostnames to each replica)
- Use `EXECUTOR_ID` env var with a startup script that derives ID from hostname
- The service discovery for peer addresses can use Docker DNS (all replicas resolve under `executor`)

**Note**: With `deploy.replicas`, all replicas share the same port internally. Peer discovery requires using Docker's internal DNS or a service registry approach rather than hardcoded addresses.

### Step 5: Vector clock update

In `utils/vector_clock.py`, add `"order_queue"` and `"executor"` to the `SERVICES` tuple (around line 3-8). This is critical — all clock operations (`new_clock`, `tick`, `merge_clocks`, `normalize_clock`) depend on this tuple being consistent across the entire system.

### Step 6: Wire orchestrator

In `orchestrator/src/app.py`, modify the checkout flow:

1. Import `order_queue_pb2` and `order_queue_pb2_grpc` (following existing path-manipulation pattern)
2. After all 3 services pass successfully (fraud detection, transaction verification, suggestions):
   - Open gRPC channel to `order_queue:50054`
   - Call `Enqueue(orderId=order_id)`
   - **Wait for `EnqueueResponse`** — only after receiving `success=True` confirmation
   - Then return the order approval response to the frontend as usual
3. If order is rejected by any service, do NOT enqueue — return rejection to frontend directly
4. Update orchestrator Dockerfile CMD to also compile `order_queue.proto`

## Context

- Port allocation: OrderQueue=50054, Executors=50055 (internal, replicated)
- Follow existing Dockerfile CMD pattern: run `protoc` at startup, then `hotreload.py`
- The queue's thread lock provides safety for concurrent Dequeue calls, but leader election is the primary mutual exclusion mechanism — only the leader should be calling Dequeue
- Vector clock integration is optional for queue/executor ops but the `SERVICES` tuple must be updated for system consistency
