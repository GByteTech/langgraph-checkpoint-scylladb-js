-- LangGraph Checkpoint Schema for ScyllaDB
-- Optimized for ScyllaDB features: clustering order, TTL, LWT, prepared statements

-- Keyspace (SimpleStrategy for single-node dev; use NetworkTopologyStrategy in production)
CREATE KEYSPACE IF NOT EXISTS langgraph
  WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

-- =============================================================================
-- Checkpoints Table
-- =============================================================================
-- Partition key: (thread_id, checkpoint_ns) — groups all checkpoints for a thread+namespace
-- Clustering key: checkpoint_id DESC — UUID6 is time-sortable, DESC gives latest first
-- This allows efficient "get latest checkpoint" with LIMIT 1 and range scans for listing.
CREATE TABLE IF NOT EXISTS langgraph.checkpoints (
  thread_id     text,
  checkpoint_ns text,
  checkpoint_id text,
  parent_checkpoint_id text,
  checkpoint    blob,       -- JSON-serialized checkpoint via serde (loadsTyped/dumpsTyped)
  metadata      blob,       -- JSON-serialized metadata via serde
  source        text,       -- denormalized from metadata for filtered queries
  step          int,        -- denormalized from metadata for filtered queries
  created_at    timestamp,
  PRIMARY KEY ((thread_id, checkpoint_ns), checkpoint_id)
) WITH CLUSTERING ORDER BY (checkpoint_id DESC)
  AND compaction = {'class': 'LeveledCompactionStrategy'}
  AND gc_grace_seconds = 86400
  AND default_time_to_live = 0;

-- =============================================================================
-- Checkpoint Writes Table
-- =============================================================================
-- Stores pending writes (intermediate state) linked to a checkpoint.
-- Partition key: (thread_id, checkpoint_ns, checkpoint_id) — colocate all writes for a checkpoint
-- Clustering key: (task_id, idx) — supports ordered retrieval of writes within a task
CREATE TABLE IF NOT EXISTS langgraph.checkpoint_writes (
  thread_id     text,
  checkpoint_ns text,
  checkpoint_id text,
  task_id       text,
  idx           int,
  channel       text,
  type          text,
  value         blob,       -- JSON-serialized write value via serde
  created_at    timestamp,
  PRIMARY KEY ((thread_id, checkpoint_ns, checkpoint_id), task_id, idx)
) WITH compaction = {'class': 'LeveledCompactionStrategy'}
  AND gc_grace_seconds = 86400
  AND default_time_to_live = 0;

-- =============================================================================
-- Secondary Indexes for filtered checkpoint listing
-- =============================================================================
-- These allow filtering checkpoints by metadata fields (source, step) in the list() method.
-- ScyllaDB secondary indexes are efficient for low-cardinality columns.
CREATE INDEX IF NOT EXISTS idx_checkpoints_source ON langgraph.checkpoints (source);
CREATE INDEX IF NOT EXISTS idx_checkpoints_step   ON langgraph.checkpoints (step);
