---
layout: default
title: Iceberg Sink Architecture
nav_order: 2
---

# Iceberg Sink Architecture

The Nobu Iceberg Sink is designed for **high-performance, low-latency streaming ingestion** into data lakes. Unlike traditional batch writers, it treats the Lakehouse as a real-time mutable store, supporting **Upserts**, **Deletes**, and **Exactly-Once** semantics.

## 🏗️ High-Level Design

The sink uses a tiered storage architecture to balance durability, latency, and throughput:

1.  **Memory Buffer**: Accumulates events in the LMAX RingBuffer.
2.  **WAL (Write-Ahead-Log)**: Persists small batches to local NVMe storage using **LMDB** for crash recovery.
3.  **Core Iceberg Committer**: Flushes batches to S3/GCS as Parquet files and commits snapshots.

```mermaid
graph TD
    Source[Event Source] -->|1. Append| WAL[Local WAL (LMDB)]
    WAL -->|2. Buffer| Mem[Memory Batcher]
    Mem -->|3. Flush| Parquet[Parquet Writer]
    Parquet -->|4. Update Index| Index[Primary Key Index]
    Index -->|5. Commit| Lake[Iceberg Table (S3)]
```

## 🔑 Primary Key Indexing

To handle **Upserts** (CDC) efficiently at scale, Nobu maintains a local **Global Index** using **LMDB**.

*   **Problem**: Standard Iceberg writers must scan remote data files to locate records for updates, which is too slow for streaming.
*   **Solution**: We maintain a KV store mapping `PrimaryKey -> (FileID, RowID)`.
*   **Performance**: This allows $O(1)$ lookups to determine if an incoming record is an **Insert** or an **Update**.

### Deletion Vectors (Iceberg V3)
Nobu leverages **Iceberg V3** features, specifically **Deletion Vectors** stored in **Puffin files**.
*   Instead of rewriting entire data files (Copy-on-Write) or managing expensive Merge-on-Read delete files, we use **RoaringBitmaps**.
*   When a row is updated, we simply "tombstone" the old position in the Deletion Vector and write the new version to a new file.

## 🔄 Handling Compaction (The Mailbox Pattern)

A critical challenge in streaming ingestion is handling **External Compaction** (e.g., Spark jobs rewriting small files) without stopping ingestion.

Nobu implements the **Mailbox Pattern** to decouple Maintenance from Ingestion:

1.  **Snapshot Monitor**: A background thread polls the Iceberg Catalog.
2.  **Detection**: When a `REPLACE` operation (compaction) is detected, it scans the **new** metadata to find moved keys.
3.  **Mailbox**: The monitor pushes a lightweight `IndexUpdateTask` to a concurrent `CompactionMailbox`.
4.  **Ingestion Thread**: Before processing the next batch of events, the main thread applies the mailbox updates to the local index.

> **Benefit**: This "Single Writer" approach eliminates complex locking, ensuring the Ingestion Thread never blocks on expensive I/O operations.

## 🛡️ Reliability & Recovery

*   **Crash Recovery**: On startup, Nobu replays the local WAL to restore any uncommitted data.
*   **At-Least-Once**: The Offset is committed to Kafka/Source only after a successful Iceberg commit.
*   **Circuit Breakers**: Ingestion throttles if the downstream Lakehouse becomes unresponsive.
