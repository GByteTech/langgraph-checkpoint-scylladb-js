import {
  afterAll,
  afterEach,
  beforeAll,
  beforeEach,
  describe,
  expect,
  it,
} from "vitest";
import {
  BaseCheckpointSaver,
  Checkpoint,
  CheckpointMetadata,
  emptyCheckpoint,
  PendingWrite,
  uuid6,
} from "@langchain/langgraph-checkpoint";
import { RunnableConfig } from "@langchain/core/runnables";
import ScylladbDriver from "scylladb-driver-alpha";
import { ScyllaDBSaver } from "../index.js";

const { Client } = ScylladbDriver;

// =============================================================================
// Integration tests running against ScyllaDB via docker-compose
// Requires: docker compose up -d --wait
// =============================================================================

const CONTACT_POINTS = [process.env.SCYLLA_HOST ?? "localhost"];
const KEYSPACE = "langgraph_test";
const LOCAL_DC = "datacenter1";

describe("ScyllaDBSaver Integration Tests", () => {
  let client: ScylladbDriver.Client;
  let saver: ScyllaDBSaver;

  beforeAll(async () => {
    // Create keyspace and tables
    const setupClient = new Client({
      contactPoints: CONTACT_POINTS,
      localDataCenter: LOCAL_DC,
    });
    await setupClient.connect();

    await setupClient.execute(`
      CREATE KEYSPACE IF NOT EXISTS ${KEYSPACE}
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}
    `);

    await setupClient.execute(`
      CREATE TABLE IF NOT EXISTS ${KEYSPACE}.checkpoints (
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
    `);

    await setupClient.execute(`
      CREATE TABLE IF NOT EXISTS ${KEYSPACE}.checkpoint_writes (
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
      )
    `);

    await setupClient.execute(
      `CREATE INDEX IF NOT EXISTS ON ${KEYSPACE}.checkpoints (source)`,
    );
    await setupClient.execute(
      `CREATE INDEX IF NOT EXISTS ON ${KEYSPACE}.checkpoints (step)`,
    );

    await setupClient.shutdown();

    // Now connect with keyspace
    client = new Client({
      contactPoints: CONTACT_POINTS,
      localDataCenter: LOCAL_DC,
      keyspace: KEYSPACE,
    });
    await client.connect();
  }, 60000);

  afterAll(async () => {
    await client?.shutdown();
  });

  beforeEach(() => {
    saver = new ScyllaDBSaver(client, {
      keyspace: KEYSPACE,
      contactPoints: CONTACT_POINTS,
      localDataCenter: LOCAL_DC,
    });
  });

  afterEach(async () => {
    // Clean up tables
    await client.execute(`TRUNCATE ${KEYSPACE}.checkpoints`);
    await client.execute(`TRUNCATE ${KEYSPACE}.checkpoint_writes`);
  });

  // ===========================================================================
  // Basic Tests
  // ===========================================================================

  it("should extend BaseCheckpointSaver", () => {
    expect(saver).toBeInstanceOf(BaseCheckpointSaver);
  });

  it("should return undefined for non-existent checkpoint", async () => {
    const result = await saver.getTuple({
      configurable: {
        thread_id: "non-existent-thread",
        checkpoint_id: "non-existent-cp",
      },
    });
    expect(result).toBeUndefined();
  });

  it("should return undefined when thread_id is missing", async () => {
    const result = await saver.getTuple({ configurable: {} });
    expect(result).toBeUndefined();
  });

  // ===========================================================================
  // put + getTuple round-trip
  // ===========================================================================

  it("should put and getTuple a checkpoint", async () => {
    const checkpoint: Checkpoint = {
      ...emptyCheckpoint(),
      id: uuid6(-1),
      channel_values: { messages: ["hello", "world"] },
      channel_versions: { messages: 1 },
    };

    const config: RunnableConfig = {
      configurable: {
        thread_id: "thread-1",
        checkpoint_ns: "",
      },
    };

    const metadata: CheckpointMetadata = {
      source: "update",
      step: 0,
      parents: {},
    };

    const savedConfig = await saver.put(config, checkpoint, metadata, {
      messages: 1,
    });

    expect(savedConfig.configurable?.checkpoint_id).toBe(checkpoint.id);

    // Retrieve by specific checkpoint_id
    const tuple = await saver.getTuple(savedConfig);
    expect(tuple).toBeDefined();
    expect(tuple!.checkpoint.id).toBe(checkpoint.id);
    expect(tuple!.checkpoint.channel_values).toEqual({
      messages: ["hello", "world"],
    });
    expect(tuple!.metadata).toEqual(metadata);
    expect(tuple!.config.configurable?.thread_id).toBe("thread-1");
    expect(tuple!.config.configurable?.checkpoint_ns).toBe("");
  });

  it("should get the latest checkpoint when no checkpoint_id specified", async () => {
    const config: RunnableConfig = {
      configurable: { thread_id: "thread-latest", checkpoint_ns: "" },
    };

    // Put two checkpoints
    const cp1: Checkpoint = {
      ...emptyCheckpoint(),
      id: uuid6(-2),
      channel_values: { step: "first" },
    };
    const cp2: Checkpoint = {
      ...emptyCheckpoint(),
      id: uuid6(-1),
      channel_values: { step: "second" },
    };

    await saver.put(
      config,
      cp1,
      { source: "input", step: 0, parents: {} },
      undefined as any,
    );
    const savedConfig2 = await saver.put(
      { configurable: { ...config.configurable, checkpoint_id: cp1.id } },
      cp2,
      { source: "update", step: 1, parents: {} },
      undefined as any,
    );

    // Get latest (no checkpoint_id)
    const latest = await saver.getTuple(config);
    expect(latest).toBeDefined();
    expect(latest!.checkpoint.id).toBe(cp2.id);
  });

  // ===========================================================================
  // put with channel filtering
  // ===========================================================================

  it("should filter channel_values based on newVersions", async () => {
    const checkpoint: Checkpoint = {
      ...emptyCheckpoint(),
      id: uuid6(-1),
      channel_values: { messages: ["hello"], counter: 42, status: "active" },
      channel_versions: { messages: 1, counter: 1, status: 1 },
    };

    const config: RunnableConfig = {
      configurable: { thread_id: "thread-filter", checkpoint_ns: "" },
    };

    // Only messages changed
    await saver.put(
      config,
      checkpoint,
      { source: "update", step: 0, parents: {} },
      { messages: 2 },
    );

    const tuple = await saver.getTuple({
      configurable: {
        thread_id: "thread-filter",
        checkpoint_ns: "",
        checkpoint_id: checkpoint.id,
      },
    });

    expect(tuple).toBeDefined();
    // Only the "messages" channel should be stored
    expect(tuple!.checkpoint.channel_values).toEqual({
      messages: ["hello"],
    });
  });

  it("should store empty channel_values when newVersions is empty", async () => {
    const checkpoint: Checkpoint = {
      ...emptyCheckpoint(),
      id: uuid6(-1),
      channel_values: { messages: ["hello"] },
      channel_versions: { messages: 1 },
    };

    const config: RunnableConfig = {
      configurable: { thread_id: "thread-empty-ver", checkpoint_ns: "" },
    };

    // Empty newVersions means no channels changed
    await saver.put(
      config,
      checkpoint,
      { source: "update", step: 0, parents: {} },
      {},
    );

    const tuple = await saver.getTuple({
      configurable: {
        thread_id: "thread-empty-ver",
        checkpoint_ns: "",
        checkpoint_id: checkpoint.id,
      },
    });

    expect(tuple).toBeDefined();
    expect(tuple!.checkpoint.channel_values).toEqual({});
  });

  // ===========================================================================
  // putWrites + pending writes retrieval
  // ===========================================================================

  it("should store and retrieve pending writes", async () => {
    const config: RunnableConfig = {
      configurable: {
        thread_id: "thread-writes",
        checkpoint_ns: "",
      },
    };

    const checkpoint: Checkpoint = {
      ...emptyCheckpoint(),
      id: uuid6(-1),
    };

    const savedConfig = await saver.put(
      config,
      checkpoint,
      { source: "update", step: 0, parents: {} },
      undefined as any,
    );

    const writes: PendingWrite[] = [
      ["channel1", "value1"],
      ["channel2", { nested: "object" }],
    ];

    await saver.putWrites(savedConfig, writes, "task-1");

    // Retrieve checkpoint — should include pending writes
    const tuple = await saver.getTuple(savedConfig);
    expect(tuple).toBeDefined();
    expect(tuple!.pendingWrites).toBeDefined();
    expect(tuple!.pendingWrites!.length).toBe(2);

    // Writes are [task_id, channel, value]
    expect(tuple!.pendingWrites![0][0]).toBe("task-1");
    expect(tuple!.pendingWrites![0][1]).toBe("channel1");
    expect(tuple!.pendingWrites![0][2]).toBe("value1");

    expect(tuple!.pendingWrites![1][0]).toBe("task-1");
    expect(tuple!.pendingWrites![1][1]).toBe("channel2");
    expect(tuple!.pendingWrites![1][2]).toEqual({ nested: "object" });
  });

  // ===========================================================================
  // list
  // ===========================================================================

  it("should list checkpoints in reverse chronological order", async () => {
    const config: RunnableConfig = {
      configurable: { thread_id: "thread-list", checkpoint_ns: "" },
    };

    const ids: string[] = [];
    for (let i = 0; i < 5; i++) {
      const cp: Checkpoint = {
        ...emptyCheckpoint(),
        id: uuid6(-1),
        channel_values: { step: i },
      };
      ids.push(cp.id);
      const parentConfig =
        i > 0
          ? {
              configurable: {
                ...config.configurable,
                checkpoint_id: ids[i - 1],
              },
            }
          : config;
      await saver.put(
        parentConfig,
        cp,
        { source: "update", step: i, parents: {} },
        undefined as any,
      );
    }

    const results: Checkpoint[] = [];
    for await (const tuple of saver.list(config)) {
      results.push(tuple.checkpoint);
    }

    expect(results.length).toBe(5);
    // Should be reverse chronological (UUID6 is time-sortable DESC)
    for (let i = 0; i < results.length - 1; i++) {
      expect(results[i].id > results[i + 1].id).toBe(true);
    }
  });

  it("should respect limit in list", async () => {
    const config: RunnableConfig = {
      configurable: { thread_id: "thread-limit", checkpoint_ns: "" },
    };

    for (let i = 0; i < 5; i++) {
      const cp: Checkpoint = {
        ...emptyCheckpoint(),
        id: uuid6(-1),
      };
      await saver.put(
        config,
        cp,
        { source: "update", step: i, parents: {} },
        undefined as any,
      );
    }

    const results: Checkpoint[] = [];
    for await (const tuple of saver.list(config, { limit: 3 })) {
      results.push(tuple.checkpoint);
    }

    expect(results.length).toBe(3);
  });

  it("should support 'before' filtering in list", async () => {
    const config: RunnableConfig = {
      configurable: { thread_id: "thread-before", checkpoint_ns: "" },
    };

    const ids: string[] = [];
    for (let i = 0; i < 5; i++) {
      const cp: Checkpoint = {
        ...emptyCheckpoint(),
        id: uuid6(-1),
      };
      ids.push(cp.id);
      await saver.put(
        config,
        cp,
        { source: "update", step: i, parents: {} },
        undefined as any,
      );
    }

    // List before the 3rd checkpoint (index 2)
    const results: Checkpoint[] = [];
    for await (const tuple of saver.list(config, {
      before: {
        configurable: {
          thread_id: "thread-before",
          checkpoint_ns: "",
          checkpoint_id: ids[2],
        },
      },
    })) {
      results.push(tuple.checkpoint);
    }

    // Should only include checkpoints before ids[2]
    for (const result of results) {
      expect(result.id < ids[2]).toBe(true);
    }
  });

  it("should support metadata filter in list", async () => {
    const config: RunnableConfig = {
      configurable: { thread_id: "thread-meta-filter", checkpoint_ns: "" },
    };

    // Create checkpoints with different sources
    for (let i = 0; i < 4; i++) {
      const cp: Checkpoint = { ...emptyCheckpoint(), id: uuid6(-1) };
      const source = i % 2 === 0 ? "input" : "update";
      await saver.put(
        config,
        cp,
        { source, step: i, parents: {} },
        undefined as any,
      );
    }

    const results: CheckpointMetadata[] = [];
    for await (const tuple of saver.list(config, {
      filter: { source: "input" },
    })) {
      results.push(tuple.metadata!);
    }

    expect(results.length).toBe(2);
    for (const meta of results) {
      expect(meta.source).toBe("input");
    }
  });

  // ===========================================================================
  // deleteThread
  // ===========================================================================

  it("should delete all data for a thread", async () => {
    const config: RunnableConfig = {
      configurable: { thread_id: "thread-delete", checkpoint_ns: "" },
    };

    // Create a checkpoint with writes
    const cp: Checkpoint = { ...emptyCheckpoint(), id: uuid6(-1) };
    const savedConfig = await saver.put(
      config,
      cp,
      { source: "update", step: 0, parents: {} },
      undefined as any,
    );

    await saver.putWrites(savedConfig, [["ch1", "val1"]], "task-1");

    // Verify data exists
    let tuple = await saver.getTuple(savedConfig);
    expect(tuple).toBeDefined();

    // Delete
    await saver.deleteThread("thread-delete");

    // Verify data is gone
    tuple = await saver.getTuple(savedConfig);
    expect(tuple).toBeUndefined();
  });

  // ===========================================================================
  // Namespace support
  // ===========================================================================

  it("should support multiple namespaces per thread", async () => {
    const threadId = "thread-ns";

    // Save checkpoints in different namespaces
    for (const ns of ["ns1", "ns2", ""]) {
      const cp: Checkpoint = { ...emptyCheckpoint(), id: uuid6(-1) };
      await saver.put(
        { configurable: { thread_id: threadId, checkpoint_ns: ns } },
        cp,
        { source: "update", step: 0, parents: {} },
        undefined as any,
      );
    }

    // Each namespace should have its own checkpoint
    for (const ns of ["ns1", "ns2", ""]) {
      const tuple = await saver.getTuple({
        configurable: { thread_id: threadId, checkpoint_ns: ns },
      });
      expect(tuple).toBeDefined();
    }

    // Listing in one namespace should not return checkpoints from another
    const results: Checkpoint[] = [];
    for await (const tuple of saver.list({
      configurable: { thread_id: threadId, checkpoint_ns: "ns1" },
    })) {
      results.push(tuple.checkpoint);
    }
    expect(results.length).toBe(1);
  });

  // ===========================================================================
  // Parent config
  // ===========================================================================

  it("should track parent checkpoint config", async () => {
    const config: RunnableConfig = {
      configurable: { thread_id: "thread-parent", checkpoint_ns: "" },
    };

    const cp1: Checkpoint = { ...emptyCheckpoint(), id: uuid6(-2) };
    const saved1 = await saver.put(
      config,
      cp1,
      { source: "input", step: 0, parents: {} },
      undefined as any,
    );

    const cp2: Checkpoint = { ...emptyCheckpoint(), id: uuid6(-1) };
    const saved2 = await saver.put(
      saved1,
      cp2,
      { source: "update", step: 1, parents: {} },
      undefined as any,
    );

    const tuple = await saver.getTuple(saved2);
    expect(tuple).toBeDefined();
    expect(tuple!.parentConfig).toBeDefined();
    expect(tuple!.parentConfig!.configurable?.checkpoint_id).toBe(cp1.id);
  });

  // ===========================================================================
  // Factory methods
  // ===========================================================================

  it("should create saver via fromConnString", async () => {
    const factorySaver = await ScyllaDBSaver.fromConnString("localhost", {
      keyspace: KEYSPACE,
      setupSchema: false,
    });

    const config: RunnableConfig = {
      configurable: { thread_id: "thread-factory", checkpoint_ns: "" },
    };

    const cp: Checkpoint = { ...emptyCheckpoint(), id: uuid6(-1) };
    const saved = await factorySaver.put(
      config,
      cp,
      { source: "update", step: 0, parents: {} },
      undefined as any,
    );

    const tuple = await factorySaver.getTuple(saved);
    expect(tuple).toBeDefined();
    expect(tuple!.checkpoint.id).toBe(cp.id);

    await factorySaver.end();
  });

  // ===========================================================================
  // Setup method
  // ===========================================================================

  it("should programmatically create schema via setup()", async () => {
    const setupSaver = new ScyllaDBSaver(client, {
      keyspace: "langgraph_setup_test",
      contactPoints: CONTACT_POINTS,
      localDataCenter: LOCAL_DC,
    });

    // setup() creates keyspace + tables
    await setupSaver.setup();

    // Verify by connecting to the new keyspace
    const verifyClient = new Client({
      contactPoints: CONTACT_POINTS,
      localDataCenter: LOCAL_DC,
      keyspace: "langgraph_setup_test",
    });
    await verifyClient.connect();

    // Should be able to query the tables
    const result = await verifyClient.execute(
      "SELECT * FROM checkpoints LIMIT 1",
    );
    expect(result).toBeDefined();

    await verifyClient.shutdown();

    // Cleanup
    const cleanupClient = new Client({
      contactPoints: CONTACT_POINTS,
      localDataCenter: LOCAL_DC,
    });
    await cleanupClient.connect();
    await cleanupClient.execute("DROP KEYSPACE IF EXISTS langgraph_setup_test");
    await cleanupClient.shutdown();
  });
});
