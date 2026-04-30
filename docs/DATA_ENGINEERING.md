# Learning Foundations: Data Engineering

This project was a practical deep-dive into the fundamental concepts that power modern data systems. Below is a mapping of how this "toy" implementation helped me build a foundation in Data Engineering.

## 1. Data Serialization & Protocols
- **Focus**: Understanding how data is structured for network transmission.
- **Project Insight**: By implementing a manual **RESP (Redis Serialization Protocol)** parser, I learned how to handle raw byte streams and the importance of efficient serialization (like length-prefixing).
- **Foundation for**: Working with standardized formats like Avro, Protobuf, or JSON in data pipelines.

## 2. Concurrency & Thread Safety
- **Focus**: Learning to manage shared state in multi-user environments.
- **Project Insight**: I used Python's `threading` and `Locks` to ensure multiple clients could read and write data without causing "race conditions" or corruption.
- **Foundation for**: Building thread-safe data ingestion services and parallel processing scripts.

## 3. Data Durability Concepts
- **Focus**: Exploring how systems prevent data loss.
- **Project Insight**: 
  - **RDB**: Learned basic binary file manipulation to save "snapshots" of state.
  - **AOF**: Explored the concept of "Write-Ahead Logging" by recording commands to an append-only file.
- **Foundation for**: Understanding database recovery protocols and the trade-offs between speed and data safety.

## 4. Distributed Systems Basics
- **Focus**: Studying how data coordinates across multiple nodes.
- **Project Insight**: I implemented a basic Master-Replica model to understand asynchronous replication and the challenges of network lag and consistency.
- **Foundation for**: Working with distributed tools like Kafka, Spark, or managed cloud databases.

## 5. Algorithmic Data Structures
- **Focus**: Choosing efficient ways to store and search data.
- **Project Insight**:
  - **Sorted Sets**: Used `bisect` for ordered storage to understand O(log N) lookup logic.
  - **Geohashing**: Explored bit-manipulation for spatial indexing.
- **Foundation for**: Optimizing data retrieval patterns and understanding database indexing.

## 6. Transactional Logic
- **Focus**: Understanding atomic operations.
- **Project Insight**: Implemented `MULTI`/`EXEC` to explore how databases group commands and use "Optimistic Locking" to handle conflicts.
- **Foundation for**: Writing reliable ETL jobs and maintaining data integrity in pipelines.

---

### Summary of Exploration:
- **Language**: Python 3.10+
- **Core Libraries**: `socket`, `threading`, `struct`, `bisect`, `math`.
- **Key Takeaway**: Building things from scratch is the best way to understand the complex systems we use every day.
