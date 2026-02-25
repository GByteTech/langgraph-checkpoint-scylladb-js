import { beforeAll, afterAll } from "vitest";
import { specTest } from "@langchain/langgraph-checkpoint-validation";
import cassandra from "cassandra-driver";
import { ScyllaDBSaver } from "../index.js";

const { Client } = cassandra;

const CONTACT_POINTS = [process.env.SCYLLA_HOST ?? "localhost"];
const KEYSPACE = "langgraph_spec";
const LOCAL_DC = "datacenter1";

let sharedClient: cassandra.Client;

specTest({
  checkpointerName: "ScyllaDBSaver",

  beforeAllTimeout: 60000,

  async beforeAll() {
    // Create keyspace and schema
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

    // Connect the shared client
    sharedClient = new Client({
      contactPoints: CONTACT_POINTS,
      localDataCenter: LOCAL_DC,
      keyspace: KEYSPACE,
    });
    await sharedClient.connect();
  },

  async afterAll() {
    await sharedClient?.shutdown();
  },

  createCheckpointer() {
    return new ScyllaDBSaver(sharedClient, {
      keyspace: KEYSPACE,
      contactPoints: CONTACT_POINTS,
      localDataCenter: LOCAL_DC,
    });
  },

  async destroyCheckpointer(_checkpointer: ScyllaDBSaver) {
    // Clean up data between test runs
    await sharedClient.execute(`TRUNCATE ${KEYSPACE}.checkpoints`);
    await sharedClient.execute(`TRUNCATE ${KEYSPACE}.checkpoint_writes`);
  },
});
