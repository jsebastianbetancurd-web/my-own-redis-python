# Redis-Inspired Key-Value Store (Python)

An educational, multi-threaded key-value store built from scratch in Python to explore the core principles of database internals. This project implements a functional subset of Redis features, including the RESP protocol, basic persistence, and replication.

![Python](https://img.shields.io/badge/python-3.10+-blue.svg)
![Redis](https://img.shields.io/badge/redis-RESP-red.svg)
![Focus](https://img.shields.io/badge/focus-learning-orange.svg)

## 🚀 Overview

This project was built to understand the "magic" behind modern data stores. By implementing a Redis clone, I explored how data travels over a network, how to manage concurrent access, and how distributed systems keep data in sync.

### What I Implemented

- **Protocol Basics**: A modular parser for the **RESP (Redis Serialization Protocol)** located in `app/resp.py`.
- **Concurrency**: A multi-threaded approach to handle multiple clients, using basic thread synchronization.
- **Data Handling**:
  - **Strings & Sets**: Working with Python-native structures to manage key-value pairs and sorted data.
  - **Streams**: Exploring append-only log structures.
- **Persistence Foundations**: 
  - **RDB & AOF**: Basic implementations of snapshotting and write-ahead logging to understand data durability.
- **Distributed Basics**:
  - **Replication**: A simple Master/Replica model to learn how commands are propagated across a network.
- **Logic & Safety**: Simple transaction handling and geohashing for spatial data.

## 🛠️ Technical Deep Dive

I documented my learning journey in these guides:

- 📖 **[Architecture & Concepts](./docs/ARCHITECTURE.md)**: A beginner-friendly breakdown of how this database works and its modular structure.
- 📊 **[Data Engineering Fundamentals](./docs/DATA_ENGINEERING.md)**: How this project helped me build a foundation in data engineering.
- 🕹️ **[Interactive Demo](./docs/DEMO.md)**: Step-by-step scenarios to see the database in action!

## 🚦 Getting Started

### Prerequisites
- Python 3.10+
- `uv` (recommended)

### Installation & Run
1. Clone the repository:
   ```bash
   git clone https://github.com/jsebastianbetancurd-web/my-own-redis-python.git
   cd codecrafters-redis-python
   ```
2. Install dependencies (for tests):
   ```bash
   uv sync
   ```
3. Run the server:
   - **Linux/macOS**: `./redis-server.sh`
   - **Windows**: `uv run -m app.main`

4. **Verify it's working**:
   Open a second terminal and run this one-liner to send a `PING` and receive a `PONG`:
   ```bash
   python -c "import socket; s=socket.socket(); s.connect(('127.0.0.1', 6379)); s.sendall(b'*1\r\n$4\r\nPING\r\n'); print(f'Server Response: {s.recv(1024).decode()}')"
   ```

### 🧪 Running Tests
The project includes a comprehensive test suite using `pytest`.
```bash
uv run pytest
```


## 📈 Learning Focus: Junior Data Engineering

This project served as a practical lab to study:
- **Data Flow**: How bytes are parsed and transformed into usable application state.
- **Concurrency**: Learning to use locks and threads to prevent data corruption.
- **Consistency**: Understanding the challenges of keeping two databases in sync.
- **Storage Trade-offs**: Comparing the pros and cons of memory vs. disk storage.

---
*Built as a learning challenge on CodeCrafters.*
