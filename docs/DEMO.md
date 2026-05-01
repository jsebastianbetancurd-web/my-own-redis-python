# 🕹️ Project Demo Guide: Interactive Redis

This guide provides step-by-step scenarios to interact with this Redis implementation. Each scenario is designed to showcase a specific "Data Engineering Foundation."

---

## Scenario 1: The "Ghost" Message (Pub/Sub)
**Goal**: Demonstrate real-time asynchronous communication and multi-client handling.

1.  **Start the Server**:
    ```bash
    ./redis-server.sh --port 6379
    ```
2.  **Open Client A (The Listener)**:
    ```bash
    redis-cli -p 6379
    127.0.0.1:6379> SUBSCRIBE global-events
    ```
3.  **Open Client B (The Broadcaster)**:
    ```bash
    redis-cli -p 6379
    127.0.0.1:6379> PUBLISH global-events "Hello from the other side!"
    ```
**What to observe**: Client A instantly receives the message. 
**Interview Talking Point**: "I used a thread-safe `pubsub_subscriptions` dictionary and a `Lock` to manage active listeners without blocking the main database thread."

---

## Scenario 2: The "Time Traveler" (Persistence Recovery)
**Goal**: Show how the system handles crashes and data durability.

1.  **Save some data**:
    ```bash
    redis-cli SET persistence-test "I survived the crash"
    ```
2.  **Kill the Server**: Press `Ctrl+C` in the server terminal.
3.  **Restart the Server**:
    ```bash
    ./redis-server.sh
    ```
4.  **Check the data**:
    ```bash
    redis-cli GET persistence-test
    ```
**What to observe**: The value returns correctly.
**Interview Talking Point**: "I implemented RDB binary parsing and AOF logging. On startup, the server automatically re-plays the logs to restore the state."

---

## Scenario 3: The "Twin Cities" (Master/Replica Sync)
**Goal**: Demonstrate distributed systems fundamentals (data propagation).

1.  **Start the Master (Port 6379)**:
    ```bash
    ./redis-server.sh --port 6379
    ```
2.  **Start the Replica (Port 6380)**:
    ```bash
    ./redis-server.sh --port 6380 --replicaof "localhost 6379"
    ```
3.  **Write to Master**:
    ```bash
    redis-cli -p 6379 SET leader "I am the Master"
    ```
4.  **Read from Replica**:
    ```bash
    redis-cli -p 6380 GET leader
    ```
**What to observe**: The replica has the data instantly.
**Interview Talking Point**: "This showcases command propagation. The master maintains a socket connection to all replicas and pushes every write command to them asynchronously."

---

## Scenario 4: The "Safe Transaction" (Optimistic Locking)
**Goal**: Show transactional integrity and ACID principles.

1.  **Start a Transaction**:
    ```bash
    redis-cli
    127.0.0.1:6379> WATCH account-balance
    127.0.0.1:6379> MULTI
    127.0.0.1:6379> SET account-balance 100
    ```
2.  **Simulate a Conflict**: (Open another terminal and run):
    ```bash
    redis-cli SET account-balance 50
    ```
3.  **Try to Execute**: (Back in the first terminal):
    ```bash
    127.0.0.1:6379> EXEC
    ```
**What to observe**: The transaction returns `(nil)` because the key was modified by another client.
**Interview Talking Point**: "This is Optimistic Concurrency Control. I implemented a version-tracking system (`key_versions`) to ensure data integrity during concurrent updates."

---

## Quick Reference Commands
- **Ping**: `redis-cli PING`
- **Geo search**: `GEOADD cities 13.36 38.11 "Palermo"`, `GEODIST cities Palermo Catania km`
- **Stream data**: `XADD mystream * sensor 1 temp 20.5`
