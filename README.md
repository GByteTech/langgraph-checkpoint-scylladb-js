# @gbyte.tech/langgraph-checkpoint-scylladb

[![CI](https://github.com/GByteTech/langgraph-checkpoint-scylladb-js/actions/workflows/ci.yml/badge.svg)](https://github.com/GByteTech/langgraph-checkpoint-scylladb-js/actions/workflows/ci.yml)
[![npm version](https://img.shields.io/npm/v/@gbyte.tech/langgraph-checkpoint-scylladb.svg)](https://www.npmjs.com/package/@gbyte.tech/langgraph-checkpoint-scylladb)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![TypeScript](https://img.shields.io/badge/TypeScript-5.x-blue.svg)](https://www.typescriptlang.org/)
[![ScyllaDB](https://img.shields.io/badge/ScyllaDB-2025.x-6DB33F.svg)](https://www.scylladb.com/)

A production-ready **ScyllaDB** checkpoint saver for [LangGraph.js](https://github.com/langchain-ai/langgraphjs). Enables persistent, high-performance state management for LangGraph agents and workflows using [ScyllaDB](https://www.scylladb.com/) — the real-time big data database compatible with Apache Cassandra.

> Built and maintained by **[GBYTE TECH](https://gbyte.tech)** — AI-powered software engineering.

---

## Features

- 🚀 **High Performance** — Leverages ScyllaDB's shard-per-core architecture for low-latency reads and writes
- 📋 **Prepared Statements** — All CQL queries use prepared statements for optimal query planning and caching
- ⏱️ **TTL Support** — Optional automatic data expiration via ScyllaDB's native `USING TTL`
- 🔒 **Idempotent Writes** — Lightweight transactions (`IF NOT EXISTS`) for safe write deduplication
- 📊 **Efficient Listing** — Clustering order `DESC` enables `O(1)` "get latest checkpoint" queries
- 🔍 **Filtered Queries** — Secondary indexes on metadata fields (`source`, `step`) for fast filtered listing
- ✅ **Fully Validated** — Passes all 710+ official `@langchain/langgraph-checkpoint-validation` spec tests
- 🏭 **Production Ready** — LeveledCompactionStrategy, configurable replication, graceful shutdown

## Installation

```bash
npm install @gbyte.tech/langgraph-checkpoint-scylladb
# or
pnpm add @gbyte.tech/langgraph-checkpoint-scylladb
# or
yarn add @gbyte.tech/langgraph-checkpoint-scylladb
```

### Peer Dependencies

```bash
npm install @langchain/core @langchain/langgraph-checkpoint
```

## Quick Start

```typescript
import { ScyllaDBSaver } from "@gbyte.tech/langgraph-checkpoint-scylladb";

// Option 1: From connection string (single node)
const saver = await ScyllaDBSaver.fromConnString("localhost", {
  keyspace: "langgraph",
  setupSchema: true, // Auto-create keyspace & tables
});

// Option 2: From full config (multi-node cluster)
const saver = await ScyllaDBSaver.fromConfig({
  contactPoints: ["node1.scylla.local", "node2.scylla.local"],
  localDataCenter: "datacenter1",
  keyspace: "langgraph",
  credentials: { username: "admin", password: "secret" },
  ttlConfig: { defaultTTLSeconds: 86400 }, // 24h TTL
});

// Option 3: BYO client (advanced)
import { Client } from "scylladb-driver-alpha";

const client = new Client({
  contactPoints: ["localhost"],
  localDataCenter: "datacenter1",
  keyspace: "langgraph",
});
await client.connect();

const saver = new ScyllaDBSaver(client);
```

### Use with LangGraph

```typescript
import { StateGraph } from "@langchain/langgraph";
import { ScyllaDBSaver } from "@gbyte.tech/langgraph-checkpoint-scylladb";

const checkpointer = await ScyllaDBSaver.fromConnString("localhost", {
  keyspace: "langgraph",
});

const graph = new StateGraph({
  /* your graph definition */
}).compile({ checkpointer });

// Run with thread persistence
const result = await graph.invoke(
  { messages: [{ role: "user", content: "Hello!" }] },
  { configurable: { thread_id: "my-thread" } },
);

// Graceful shutdown
await checkpointer.end();
```

## Schema

The library uses two tables optimized for ScyllaDB's distributed architecture:

```sql
-- Checkpoints: partitioned by (thread_id, namespace), clustered by checkpoint_id DESC
CREATE TABLE checkpoints (
  thread_id text, checkpoint_ns text, checkpoint_id text,
  parent_checkpoint_id text, checkpoint blob, metadata blob,
  source text, step int, created_at timestamp,
  PRIMARY KEY ((thread_id, checkpoint_ns), checkpoint_id)
) WITH CLUSTERING ORDER BY (checkpoint_id DESC);

-- Writes: partitioned by (thread_id, namespace, checkpoint_id), clustered by task
CREATE TABLE checkpoint_writes (
  thread_id text, checkpoint_ns text, checkpoint_id text,
  task_id text, idx int, channel text, type text,
  value blob, created_at timestamp,
  PRIMARY KEY ((thread_id, checkpoint_ns, checkpoint_id), task_id, idx)
);
```

### Schema Setup

**Option A**: Docker Compose (development)

```bash
docker compose up -d  # Applies docker/scylla-init.cql automatically
```

**Option B**: Programmatic (production)

```typescript
const saver = await ScyllaDBSaver.fromConnString("localhost", {
  setupSchema: true, // Creates keyspace, tables, and indexes
});
```

**Option C**: Manual CQL — see [`docker/scylla-init.cql`](docker/scylla-init.cql)

## API Reference

### `ScyllaDBSaver`

Extends [`BaseCheckpointSaver`](https://langchain-ai.github.io/langgraphjs/reference/classes/checkpoint.BaseCheckpointSaver.html) from `@langchain/langgraph-checkpoint`.

| Method                                           | Description                              |
| ------------------------------------------------ | ---------------------------------------- |
| `getTuple(config)`                               | Get a specific or latest checkpoint      |
| `put(config, checkpoint, metadata, newVersions)` | Save a checkpoint                        |
| `putWrites(config, writes, taskId)`              | Store pending writes for a checkpoint    |
| `list(config, options?)`                         | List checkpoints with optional filtering |
| `deleteThread(threadId)`                         | Delete all data for a thread             |
| `setup()`                                        | Create keyspace, tables, and indexes     |
| `end()`                                          | Gracefully shut down the connection      |

### Static Factory Methods

| Method                                         | Description                     |
| ---------------------------------------------- | ------------------------------- |
| `ScyllaDBSaver.fromConfig(config, options?)`   | Create from full cluster config |
| `ScyllaDBSaver.fromConnString(host, options?)` | Create from single host string  |

### Configuration

```typescript
interface TTLConfig {
  defaultTTLSeconds?: number; // TTL in seconds for all inserts
}

interface ScyllaDBSaverConfig {
  contactPoints: string[]; // ScyllaDB cluster nodes
  localDataCenter: string; // DC name (e.g., "datacenter1")
  keyspace?: string; // Default: "langgraph"
  ttlConfig?: TTLConfig; // Optional TTL
  credentials?: { username; password }; // Optional auth
}
```

## Development

### Prerequisites

- Node.js ≥ 18
- Docker & Docker Compose
- pnpm

### Setup

```bash
git clone https://github.com/GByteTech/langgraph-checkpoint-scylladb-js.git
cd langgraph-checkpoint-scylladb-js
pnpm install
```

### Running Tests

```bash
# Start ScyllaDB
docker compose up -d --wait

# Run all tests (integration + spec validation)
pnpm test

# Watch mode
pnpm test:watch

# Build
pnpm build
```

### Test Suite

| Suite           | Tests   | Description                                                             |
| --------------- | ------- | ----------------------------------------------------------------------- |
| Integration     | 17      | Custom tests for all CRUD operations, TTL, namespaces, factory methods  |
| Spec Validation | 710     | Official `@langchain/langgraph-checkpoint-validation` conformance suite |
| **Total**       | **727** | **All passing ✅**                                                      |

## ScyllaDB vs Other Backends

| Feature                | ScyllaDB    | Redis          | SQLite         | PostgreSQL |
| ---------------------- | ----------- | -------------- | -------------- | ---------- |
| Horizontal scalability | ✅ Linear   | ❌ Single-node | ❌ Single-file | ⚠️ Complex |
| Latency (p99)          | < 1ms       | < 1ms          | ~1ms           | ~5ms       |
| Data persistence       | ✅ Durable  | ⚠️ AOF/RDB     | ✅ Durable     | ✅ Durable |
| TTL support            | ✅ Native   | ✅ Native      | ❌ Manual      | ❌ Manual  |
| Multi-DC replication   | ✅ Built-in | ❌             | ❌             | ⚠️ Complex |
| Operational overhead   | Medium      | Low            | None           | Medium     |

## Contributing

We welcome contributions! Please see [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines.

## License

[MIT](LICENSE) — see [LICENSE](LICENSE) for details.

---

<p align="center">
  Developed by <a href="https://gbyte.tech"><strong>GBYTE TECH</strong></a> · AI-Powered Software Engineering
</p>
