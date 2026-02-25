import {
  BaseCheckpointSaver,
  type ChannelVersions,
  type Checkpoint,
  type CheckpointListOptions,
  type CheckpointMetadata,
  type CheckpointTuple,
  type PendingWrite,
  copyCheckpoint,
  WRITES_IDX_MAP,
  uuid6,
} from "@langchain/langgraph-checkpoint";
import type { RunnableConfig } from "@langchain/core/runnables";
import cassandra from "cassandra-driver";

const { Client, types, mapping } = cassandra;

// Re-export for convenience
export type CassandraClient = cassandra.Client;

/**
 * TTL configuration for automatic data expiration.
 */
export interface TTLConfig {
  /** TTL in seconds. Applied to all inserts via USING TTL. */
  defaultTTLSeconds?: number;
}

/**
 * Configuration for creating a ScyllaDBSaver instance.
 */
export interface ScyllaDBSaverConfig {
  /** Contact points (hosts) for the ScyllaDB cluster. */
  contactPoints: string[];
  /** Local data center name. */
  localDataCenter: string;
  /** Keyspace to use. Defaults to "langgraph". */
  keyspace?: string;
  /** Optional TTL configuration. */
  ttlConfig?: TTLConfig;
  /** Optional credentials. */
  credentials?: { username: string; password: string };
}

// =============================================================================
// CQL Queries (all use prepared statements for performance)
// =============================================================================

const CQL = {
  // --- Checkpoints ---
  GET_CHECKPOINT: `
    SELECT thread_id, checkpoint_ns, checkpoint_id, parent_checkpoint_id,
           checkpoint, metadata, source, step, created_at
    FROM checkpoints
    WHERE thread_id = ? AND checkpoint_ns = ? AND checkpoint_id = ?
  `,

  GET_LATEST_CHECKPOINT: `
    SELECT thread_id, checkpoint_ns, checkpoint_id, parent_checkpoint_id,
           checkpoint, metadata, source, step, created_at
    FROM checkpoints
    WHERE thread_id = ? AND checkpoint_ns = ?
    LIMIT 1
  `,

  PUT_CHECKPOINT: `
    INSERT INTO checkpoints (
      thread_id, checkpoint_ns, checkpoint_id, parent_checkpoint_id,
      checkpoint, metadata, source, step, created_at
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, toTimestamp(now()))
  `,

  PUT_CHECKPOINT_TTL: `
    INSERT INTO checkpoints (
      thread_id, checkpoint_ns, checkpoint_id, parent_checkpoint_id,
      checkpoint, metadata, source, step, created_at
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, toTimestamp(now()))
    USING TTL ?
  `,

  LIST_CHECKPOINTS: `
    SELECT thread_id, checkpoint_ns, checkpoint_id, parent_checkpoint_id,
           checkpoint, metadata, source, step, created_at
    FROM checkpoints
    WHERE thread_id = ? AND checkpoint_ns = ?
    ORDER BY checkpoint_id DESC
  `,

  LIST_CHECKPOINTS_BEFORE: `
    SELECT thread_id, checkpoint_ns, checkpoint_id, parent_checkpoint_id,
           checkpoint, metadata, source, step, created_at
    FROM checkpoints
    WHERE thread_id = ? AND checkpoint_ns = ? AND checkpoint_id < ?
    ORDER BY checkpoint_id DESC
  `,

  // Global listing (no partition key constraints) — requires ALLOW FILTERING
  LIST_ALL_CHECKPOINTS: `
    SELECT thread_id, checkpoint_ns, checkpoint_id, parent_checkpoint_id,
           checkpoint, metadata, source, step, created_at
    FROM checkpoints
    ALLOW FILTERING
  `,

  // List by checkpoint_ns only (across all threads)
  LIST_CHECKPOINTS_BY_NS: `
    SELECT thread_id, checkpoint_ns, checkpoint_id, parent_checkpoint_id,
           checkpoint, metadata, source, step, created_at
    FROM checkpoints
    WHERE checkpoint_ns = ?
    ALLOW FILTERING
  `,

  DELETE_CHECKPOINTS: `
    DELETE FROM checkpoints
    WHERE thread_id = ? AND checkpoint_ns = ?
  `,

  LIST_CHECKPOINT_NS: `
    SELECT DISTINCT thread_id, checkpoint_ns
    FROM checkpoints
    WHERE thread_id = ?
    ALLOW FILTERING
  `,

  // --- Checkpoint Writes ---
  GET_WRITES: `
    SELECT task_id, idx, channel, type, value
    FROM checkpoint_writes
    WHERE thread_id = ? AND checkpoint_ns = ? AND checkpoint_id = ?
    ORDER BY task_id ASC, idx ASC
  `,

  PUT_WRITE: `
    INSERT INTO checkpoint_writes (
      thread_id, checkpoint_ns, checkpoint_id, task_id, idx,
      channel, type, value, created_at
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, toTimestamp(now()))
    IF NOT EXISTS
  `,

  PUT_WRITE_TTL: `
    INSERT INTO checkpoint_writes (
      thread_id, checkpoint_ns, checkpoint_id, task_id, idx,
      channel, type, value, created_at
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, toTimestamp(now()))
    USING TTL ?
  `,

  DELETE_WRITES: `
    DELETE FROM checkpoint_writes
    WHERE thread_id = ? AND checkpoint_ns = ? AND checkpoint_id = ?
  `,

  LIST_WRITE_CHECKPOINTS: `
    SELECT DISTINCT thread_id, checkpoint_ns, checkpoint_id
    FROM checkpoint_writes
    WHERE thread_id = ? AND checkpoint_ns = ?
    ALLOW FILTERING
  `,
} as const;

// =============================================================================
// Schema setup CQL (used by setup() method)
// =============================================================================

function getSetupCQL(keyspace: string): string[] {
  return [
    `CREATE KEYSPACE IF NOT EXISTS ${keyspace} WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}`,

    `CREATE TABLE IF NOT EXISTS ${keyspace}.checkpoints (
      thread_id text,
      checkpoint_ns text,
      checkpoint_id text,
      parent_checkpoint_id text,
      checkpoint blob,
      metadata blob,
      source text,
      step int,
      created_at timestamp,
      PRIMARY KEY ((thread_id, checkpoint_ns), checkpoint_id)
    ) WITH CLUSTERING ORDER BY (checkpoint_id DESC)
      AND compaction = {'class': 'LeveledCompactionStrategy'}
      AND gc_grace_seconds = 86400`,

    `CREATE TABLE IF NOT EXISTS ${keyspace}.checkpoint_writes (
      thread_id text,
      checkpoint_ns text,
      checkpoint_id text,
      task_id text,
      idx int,
      channel text,
      type text,
      value blob,
      created_at timestamp,
      PRIMARY KEY ((thread_id, checkpoint_ns, checkpoint_id), task_id, idx)
    ) WITH compaction = {'class': 'LeveledCompactionStrategy'}
      AND gc_grace_seconds = 86400`,

    `CREATE INDEX IF NOT EXISTS idx_checkpoints_source ON ${keyspace}.checkpoints (source)`,
    `CREATE INDEX IF NOT EXISTS idx_checkpoints_step ON ${keyspace}.checkpoints (step)`,
  ];
}

/**
 * ScyllaDB-backed checkpoint saver for LangGraph.
 *
 * Uses ScyllaDB features:
 * - Prepared statements for all queries
 * - Clustering order DESC for efficient "get latest" queries
 * - Optional TTL for automatic data expiration
 * - Lightweight transactions (IF NOT EXISTS) for idempotent writes
 * - Composite primary keys for efficient partition-scoped range scans
 * - LeveledCompactionStrategy for read-optimized workloads
 */
export class ScyllaDBSaver extends BaseCheckpointSaver {
  private client: CassandraClient;
  private ttlConfig?: TTLConfig;
  private keyspace: string;
  private contactPoints: string[];
  private localDataCenter: string;
  private credentials?: { username: string; password: string };

  constructor(
    client: CassandraClient,
    config?: {
      ttlConfig?: TTLConfig;
      keyspace?: string;
      contactPoints?: string[];
      localDataCenter?: string;
      credentials?: { username: string; password: string };
    },
  ) {
    super();
    this.client = client;
    this.ttlConfig = config?.ttlConfig;
    this.keyspace = config?.keyspace ?? "langgraph";
    this.contactPoints = config?.contactPoints ?? ["localhost"];
    this.localDataCenter = config?.localDataCenter ?? "datacenter1";
    this.credentials = config?.credentials;
  }

  /**
   * Create a ScyllaDBSaver from a configuration object.
   * Connects the client and optionally sets up the schema.
   */
  static async fromConfig(
    config: ScyllaDBSaverConfig,
    options?: { setupSchema?: boolean },
  ): Promise<ScyllaDBSaver> {
    const keyspace = config.keyspace ?? "langgraph";
    const clientConfig: cassandra.ClientOptions = {
      contactPoints: config.contactPoints,
      localDataCenter: config.localDataCenter,
      keyspace,
    };

    if (config.credentials) {
      clientConfig.credentials = config.credentials;
    }

    const client = new Client(clientConfig);
    await client.connect();

    const saver = new ScyllaDBSaver(client, {
      ttlConfig: config.ttlConfig,
      keyspace,
      contactPoints: config.contactPoints,
      localDataCenter: config.localDataCenter,
      credentials: config.credentials,
    });

    if (options?.setupSchema) {
      await saver.setup();
    }

    return saver;
  }

  /**
   * Create a ScyllaDBSaver from a connection string (convenience for single-node).
   */
  static async fromConnString(
    host: string,
    options?: {
      keyspace?: string;
      ttlConfig?: TTLConfig;
      setupSchema?: boolean;
    },
  ): Promise<ScyllaDBSaver> {
    return ScyllaDBSaver.fromConfig(
      {
        contactPoints: [host],
        localDataCenter: "datacenter1",
        keyspace: options?.keyspace,
        ttlConfig: options?.ttlConfig,
      },
      { setupSchema: options?.setupSchema },
    );
  }

  /**
   * Programmatically create keyspace, tables, and indexes.
   * Safe to call multiple times (uses IF NOT EXISTS).
   */
  async setup(): Promise<void> {
    // Need a client without keyspace to create the keyspace itself
    const setupClientConfig: cassandra.ClientOptions = {
      contactPoints: this.contactPoints,
      localDataCenter: this.localDataCenter,
    };
    if (this.credentials) {
      setupClientConfig.credentials = this.credentials;
    }
    const setupClient = new Client(setupClientConfig);
    await setupClient.connect();

    try {
      const statements = getSetupCQL(this.keyspace);
      for (const cql of statements) {
        await setupClient.execute(cql);
      }
    } finally {
      await setupClient.shutdown();
    }
  }

  // ===========================================================================
  // BaseCheckpointSaver implementation
  // ===========================================================================

  async getTuple(config: RunnableConfig): Promise<CheckpointTuple | undefined> {
    const threadId = config.configurable?.thread_id;
    const checkpointNs = config.configurable?.checkpoint_ns ?? "";
    const checkpointId = config.configurable?.checkpoint_id;

    if (!threadId) {
      return undefined;
    }

    let row: cassandra.types.Row | null = null;

    if (checkpointId) {
      const result = await this.client.execute(
        CQL.GET_CHECKPOINT,
        [threadId, checkpointNs, checkpointId],
        { prepare: true },
      );
      row = result.rows[0] ?? null;
    } else {
      const result = await this.client.execute(
        CQL.GET_LATEST_CHECKPOINT,
        [threadId, checkpointNs],
        { prepare: true },
      );
      row = result.rows[0] ?? null;
    }

    if (!row) {
      return undefined;
    }

    return this.rowToCheckpointTuple(row);
  }

  async put(
    config: RunnableConfig,
    checkpoint: Checkpoint,
    metadata: CheckpointMetadata,
    newVersions: ChannelVersions,
  ): Promise<RunnableConfig> {
    const threadId = config.configurable?.thread_id;
    const checkpointNs = config.configurable?.checkpoint_ns ?? "";
    const parentCheckpointId = config.configurable?.checkpoint_id;

    if (!threadId) {
      throw new Error("thread_id is required in config.configurable");
    }

    const checkpointId = checkpoint.id || uuid6(0);

    // Copy checkpoint and filter channel_values based on newVersions
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    const storedCheckpoint: any = { ...copyCheckpoint(checkpoint) };
    // Preserve pending_sends if present (for v < 4 checkpoints)
    if ("pending_sends" in checkpoint) {
      storedCheckpoint.pending_sends = (checkpoint as any).pending_sends;
    }
    if (storedCheckpoint.channel_values && newVersions !== undefined) {
      if (Object.keys(newVersions).length === 0) {
        storedCheckpoint.channel_values = {};
      } else {
        const filtered: Record<string, unknown> = {};
        for (const channel of Object.keys(newVersions)) {
          if (channel in storedCheckpoint.channel_values) {
            filtered[channel] = storedCheckpoint.channel_values[channel];
          }
        }
        storedCheckpoint.channel_values = filtered;
      }
    }

    // Serialize checkpoint and metadata via serde
    const [, checkpointBytes] = await this.serde.dumpsTyped(storedCheckpoint);
    const [, metadataBytes] = await this.serde.dumpsTyped(metadata);

    // Extract searchable metadata fields
    const source =
      metadata && "source" in metadata ? (metadata.source as string) : null;
    const step =
      metadata && "step" in metadata ? (metadata.step as number) : null;

    const params: unknown[] = [
      threadId,
      checkpointNs,
      checkpointId,
      parentCheckpointId ?? null,
      Buffer.from(checkpointBytes),
      Buffer.from(metadataBytes),
      source,
      step,
    ];

    if (this.ttlConfig?.defaultTTLSeconds) {
      params.push(this.ttlConfig.defaultTTLSeconds);
      await this.client.execute(CQL.PUT_CHECKPOINT_TTL, params, {
        prepare: true,
      });
    } else {
      await this.client.execute(CQL.PUT_CHECKPOINT, params, {
        prepare: true,
      });
    }

    return {
      configurable: {
        thread_id: threadId,
        checkpoint_ns: checkpointNs,
        checkpoint_id: checkpointId,
      },
    };
  }

  async *list(
    config: RunnableConfig,
    options?: CheckpointListOptions,
  ): AsyncGenerator<CheckpointTuple> {
    const threadId = config.configurable?.thread_id;
    const checkpointNs = config.configurable?.checkpoint_ns;

    const limit = options?.limit ?? 10;
    const beforeConfig = options?.before;
    const beforeCheckpointId = beforeConfig?.configurable?.checkpoint_id;

    let allRows: cassandra.types.Row[] = [];

    if (threadId && checkpointNs !== undefined) {
      // Case 1: Both thread_id and checkpoint_ns specified — efficient partition query
      let query: string;
      let params: unknown[];

      if (beforeCheckpointId) {
        query = CQL.LIST_CHECKPOINTS_BEFORE;
        params = [threadId, checkpointNs, beforeCheckpointId];
      } else {
        query = CQL.LIST_CHECKPOINTS;
        params = [threadId, checkpointNs];
      }

      const result = await this.client.execute(query, params, {
        prepare: true,
        fetchSize: 5000,
      });
      allRows = result.rows;
    } else if (threadId && checkpointNs === undefined) {
      // Case 2: thread_id specified but checkpoint_ns is undefined — search all namespaces
      const nsResult = await this.client.execute(
        CQL.LIST_CHECKPOINT_NS,
        [threadId],
        { prepare: true },
      );
      const namespaces = nsResult.rows.map(
        (row) => row["checkpoint_ns"] as string,
      );

      for (const ns of namespaces) {
        let query: string;
        let params: unknown[];

        if (beforeCheckpointId) {
          query = CQL.LIST_CHECKPOINTS_BEFORE;
          params = [threadId, ns, beforeCheckpointId];
        } else {
          query = CQL.LIST_CHECKPOINTS;
          params = [threadId, ns];
        }

        const result = await this.client.execute(query, params, {
          prepare: true,
          fetchSize: 5000,
        });
        allRows.push(...result.rows);
      }

      // Sort merged results by checkpoint_id DESC (UUID6 is time-sortable)
      allRows.sort((a, b) => {
        const aId = a["checkpoint_id"] as string;
        const bId = b["checkpoint_id"] as string;
        return bId.localeCompare(aId);
      });
    } else if (!threadId && checkpointNs !== undefined) {
      // Case 3: No thread_id but checkpoint_ns specified — scan by ns
      const result = await this.client.execute(
        CQL.LIST_CHECKPOINTS_BY_NS,
        [checkpointNs],
        { prepare: true, fetchSize: 5000 },
      );
      allRows = result.rows;

      // Sort by checkpoint_id DESC
      allRows.sort((a, b) => {
        const aId = a["checkpoint_id"] as string;
        const bId = b["checkpoint_id"] as string;
        return bId.localeCompare(aId);
      });
    } else {
      // Case 4: Neither thread_id nor checkpoint_ns — global scan
      const result = await this.client.execute(CQL.LIST_ALL_CHECKPOINTS, [], {
        prepare: true,
        fetchSize: 5000,
      });
      allRows = result.rows;

      // Sort by checkpoint_id DESC
      allRows.sort((a, b) => {
        const aId = a["checkpoint_id"] as string;
        const bId = b["checkpoint_id"] as string;
        return bId.localeCompare(aId);
      });
    }

    let yieldedCount = 0;

    for (const row of allRows) {
      if (yieldedCount >= limit) break;

      // Apply 'before' filter for cross-namespace/global cases
      if (beforeCheckpointId && (!threadId || checkpointNs === undefined)) {
        const rowCheckpointId = row["checkpoint_id"] as string;
        if (rowCheckpointId >= beforeCheckpointId) {
          continue;
        }
      }

      // Apply metadata filter if provided
      if (options?.filter && Object.keys(options.filter).length > 0) {
        if (!this.matchesFilter(row, options.filter)) {
          continue;
        }
      }

      const tuple = await this.rowToCheckpointTuple(row);
      yield tuple;
      yieldedCount++;
    }
  }

  async putWrites(
    config: RunnableConfig,
    writes: PendingWrite[],
    taskId: string,
  ): Promise<void> {
    const threadId = config.configurable?.thread_id;
    const checkpointNs = config.configurable?.checkpoint_ns ?? "";
    const checkpointId = config.configurable?.checkpoint_id;

    if (!threadId || !checkpointId) {
      throw new Error(
        "thread_id and checkpoint_id are required in config.configurable",
      );
    }

    // Execute writes individually with IF NOT EXISTS (LWT) for idempotency.
    // ScyllaDB LWT doesn't work inside batches the same way, so we execute individually.
    for (let i = 0; i < writes.length; i++) {
      const [channel, value] = writes[i];

      // Map special channels to negative indices
      const idx =
        WRITES_IDX_MAP[channel] !== undefined ? WRITES_IDX_MAP[channel] : i;

      const type = typeof value === "object" ? "json" : "string";
      const [, serializedValue] = await this.serde.dumpsTyped(value);

      const params: unknown[] = [
        threadId,
        checkpointNs,
        checkpointId,
        taskId,
        idx,
        channel,
        type,
        Buffer.from(serializedValue),
      ];

      if (this.ttlConfig?.defaultTTLSeconds) {
        // TTL variant does not use IF NOT EXISTS (CQL doesn't allow both)
        params.push(this.ttlConfig.defaultTTLSeconds);
        await this.client.execute(CQL.PUT_WRITE_TTL, params, {
          prepare: true,
        });
      } else {
        await this.client.execute(CQL.PUT_WRITE, params, {
          prepare: true,
        });
      }
    }
  }

  async deleteThread(threadId: string): Promise<void> {
    // First, find all checkpoint_ns values for this thread
    const nsResult = await this.client.execute(
      CQL.LIST_CHECKPOINT_NS,
      [threadId],
      { prepare: true },
    );

    const namespaces = nsResult.rows.map(
      (row) => row["checkpoint_ns"] as string,
    );

    // Delete checkpoints and writes for each namespace
    for (const ns of namespaces) {
      // Get all checkpoint_ids for this thread+ns to delete writes
      const cpResult = await this.client.execute(
        CQL.LIST_CHECKPOINTS,
        [threadId, ns],
        { prepare: true },
      );

      // Delete writes for each checkpoint
      for (const cpRow of cpResult.rows) {
        const checkpointId = cpRow["checkpoint_id"] as string;
        await this.client.execute(
          CQL.DELETE_WRITES,
          [threadId, ns, checkpointId],
          { prepare: true },
        );
      }

      // Delete all checkpoints for this thread+ns
      await this.client.execute(CQL.DELETE_CHECKPOINTS, [threadId, ns], {
        prepare: true,
      });
    }
  }

  /**
   * Gracefully shut down the ScyllaDB client connection.
   */
  async end(): Promise<void> {
    await this.client.shutdown();
  }

  // ===========================================================================
  // Private helpers
  // ===========================================================================

  private async rowToCheckpointTuple(
    row: cassandra.types.Row,
  ): Promise<CheckpointTuple> {
    const threadId = row["thread_id"] as string;
    const checkpointNs = row["checkpoint_ns"] as string;
    const checkpointId = row["checkpoint_id"] as string;
    const parentCheckpointId = row["parent_checkpoint_id"] as string | null;

    // Deserialize checkpoint and metadata via serde
    const checkpointBlob = row["checkpoint"] as Buffer;
    const metadataBlob = row["metadata"] as Buffer;

    const checkpoint: Checkpoint = (await this.serde.loadsTyped(
      "json",
      new Uint8Array(checkpointBlob),
    )) as Checkpoint;

    const metadata: CheckpointMetadata = (await this.serde.loadsTyped(
      "json",
      new Uint8Array(metadataBlob),
    )) as CheckpointMetadata;

    // Load pending writes
    const pendingWrites = await this.loadPendingWrites(
      threadId,
      checkpointNs,
      checkpointId,
    );

    const tuple: CheckpointTuple = {
      config: {
        configurable: {
          thread_id: threadId,
          checkpoint_ns: checkpointNs,
          checkpoint_id: checkpointId,
        },
      },
      checkpoint,
      metadata,
      pendingWrites: pendingWrites.length > 0 ? pendingWrites : undefined,
    };

    if (parentCheckpointId) {
      tuple.parentConfig = {
        configurable: {
          thread_id: threadId,
          checkpoint_ns: checkpointNs,
          checkpoint_id: parentCheckpointId,
        },
      };
    }

    return tuple;
  }

  private async loadPendingWrites(
    threadId: string,
    checkpointNs: string,
    checkpointId: string,
  ): Promise<Array<[string, string, unknown]>> {
    const result = await this.client.execute(
      CQL.GET_WRITES,
      [threadId, checkpointNs, checkpointId],
      { prepare: true },
    );

    const writes: Array<[string, string, unknown]> = [];

    for (const row of result.rows) {
      const taskId = row["task_id"] as string;
      const channel = row["channel"] as string;
      const valueBlob = row["value"] as Buffer;

      const value = await this.serde.loadsTyped(
        "json",
        new Uint8Array(valueBlob),
      );

      writes.push([taskId, channel, value]);
    }

    return writes;
  }

  private matchesFilter(
    row: cassandra.types.Row,
    filter: Record<string, unknown>,
  ): boolean {
    // First try to match against denormalized columns (source, step)
    for (const [key, value] of Object.entries(filter)) {
      if (value === undefined) continue;

      if (key === "source") {
        const rowSource = row["source"] as string | null;
        if (value === null) {
          if (rowSource !== null) return false;
        } else if (rowSource !== value) {
          return false;
        }
      } else if (key === "step") {
        const rowStep = row["step"] as number | null;
        if (value === null) {
          if (rowStep !== null) return false;
        } else if (rowStep !== value) {
          return false;
        }
      } else {
        // For other metadata fields, we need to deserialize and check
        const metadataBlob = row["metadata"] as Buffer;
        if (!metadataBlob) return false;

        try {
          const metadata = JSON.parse(
            new TextDecoder().decode(new Uint8Array(metadataBlob)),
          );
          const metaValue = metadata[key];
          if (value === null) {
            if (metaValue !== null) return false;
          } else if (typeof value === "object") {
            if (JSON.stringify(metaValue) !== JSON.stringify(value))
              return false;
          } else if (metaValue !== value) {
            return false;
          }
        } catch {
          return false;
        }
      }
    }

    return true;
  }
}
