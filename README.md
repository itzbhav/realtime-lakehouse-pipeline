# Phase II — Real-Time Lakehouse (local Docker)

Built incrementally, one week at a time.

- **Week 1** (done): Kafka (KRaft) → PySpark Structured Streaming → local Parquet + JSON dead-letter.
- **Week 2** (this update): valid records now land as a **Delta Lake table on MinIO**, catalogued in **Hive Metastore**. Dead-letter path is unchanged.

No Trino, Airflow, or Metabase yet — those come in later weeks.

## Stack

| Service | Image | Notes |
|---|---|---|
| `kafka` | `apache/kafka:3.7.2` | KRaft mode, single node (broker+controller combined) |
| `spark` | `bitnamilegacy/spark:3.5.6` | Kept idle (`sleep infinity`); we `docker exec` into it to run `spark-submit`. `bitnami/spark` was paywalled by Broadcom in 2025 — `bitnamilegacy/spark` is the still-free frozen mirror. |
| `minio` | `quay.io/minio/minio:RELEASE.2025-09-07T16-13-09Z` | S3-compatible object storage. `minio/minio` was pulled from Docker Hub in 2025 — `quay.io/minio/minio` is the maintained distribution now. |
| `minio-init` | `quay.io/minio/mc:RELEASE.2025-08-13T08-35-41Z` | One-shot: creates the `lakehouse` bucket, then exits. |
| `postgres` | `postgres:16.15` | Backing DB for Hive Metastore. **Ephemeral (no volume)** — Hive's own `schematool -initSchema` isn't idempotent (it errors "relation already exists" on a re-init), so a persisted Postgres volume breaks the metastore on every `docker compose down && up`. Since the metastore only holds catalog *pointers* (the real data + history lives in Delta's own log on MinIO), this is a safe simplification: `stream_ingest.py`'s `CREATE TABLE IF NOT EXISTS` just re-registers the table on next run. |
| `hive-warehouse-init` | `busybox:1.36` | One-shot: `chown`s the shared `hive_warehouse` volume to the metastore's `hive` user (uid 1000) before it starts — see below. |
| `hive-metastore` | `apache/hive:3.1.3` | Standalone metastore (`SERVICE_NAME=metastore`), Thrift on port 9083. Pinned to **3.1.3 specifically** — Spark 3.5.6 only officially supports Hive Metastore versions up to 3.1.3 (verified in Spark's own source); the newer `standalone-metastore-4.x` tags are not compatible with this Spark version. |

Spark packages used by the streaming job:
- `org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.6` (Week 1, Kafka connector — pinned to match Spark exactly)
- `io.delta:delta-spark_2.12:3.3.3` (Week 2, Delta Lake — 3.3.x is Delta's latest line supporting Spark 3.5.x, Scala 2.12 matches Spark 3.5.6's build)
- `org.apache.hadoop:hadoop-aws:3.3.4` (Week 2, S3A — pinned to match the Hadoop client version Spark 3.5.6 bundles; its transitive `aws-java-sdk-bundle:1.12.262` is resolved automatically rather than hand-pinned)

**On the Hive client version:** the plan called for Spark to download a matching Hive 3.1.3 metastore client (`spark.sql.hive.metastore.jars=maven`) to talk to the 3.1.3 server precisely. In testing this pulled a genuinely enormous transitive dependency tree (Tez, LLAP, HBase, ZooKeeper — `hive-exec`'s full kitchen sink) that was slow and flaky against Maven Central, and its *isolated* classloader turned out not to share Spark's own `hadoop-aws`, breaking S3A access from within Hive-side operations. Switched to `spark.sql.hive.metastore.jars=builtin` — Spark's bundled Hive 2.3.9 client, no extra download, no classloader isolation. It talks Thrift to the 3.1.3 server without issue for the DDL this job does (`CREATE DATABASE`, `CREATE TABLE ... LOCATION`), and it's the far more common real-world setup.

**On the shared `hive_warehouse` volume:** `CREATE DATABASE` computes a default directory from `spark.sql.warehouse.dir` and sends it to the metastore, which creates that directory **on the server side** (`hive-metastore` container) since Hive Metastore is a remote/Thrift service, not embedded in the Spark process. So that path has to actually exist and be writable *inside the hive-metastore container* — a plain local path from the Spark container (e.g. `/app/spark-warehouse`) doesn't exist there at all, and pointing it at S3 hits the same classloader isolation as above (the metastore container has no S3A jars). The fix: a small shared Docker volume (`hive_warehouse`) mounted at `/warehouse` in both containers, used only for this lightweight database-metadata directory — the actual table data still goes to `s3a://lakehouse/...` via the table's explicit `LOCATION`. `hive-warehouse-init` chowns it to uid 1000 once, since the metastore process runs as its own `hive` user, not root, and a fresh Docker volume defaults to root ownership.

## Topics

- `respiratory_events` — producer writes here (auto-created on first publish)
- No separate DLQ topic — invalid records are written to a local `output/dead_letter/` JSON folder instead (unchanged since Week 1)

## Event schema

```json
{
  "event_id": "uuid string",
  "patient_id": "P-0001",
  "diagnosis": "COPD | Asthma | Pneumonia | Bronchitis | COVID-19 | Normal",
  "probe_type": "SpO2 | Spirometer | Capnograph | Nasal Cannula",
  "severity": "Low | Medium | High | Critical",
  "event_ts": "2026-09-17T12:00:00.123Z"
}
```

`producer.py` intentionally corrupts ~5% of records (configurable via `--malformed-rate`), rotating through: a missing required field, an invalid `severity`, an unparseable `event_ts`, an invalid `diagnosis`, or a fully garbled (non-JSON) payload. **Unchanged this week.**

## Valid vs. dead-letter separation

`stream_ingest.py`:
1. Applies an **explicit `StructType`** to the raw Kafka value via `from_json` (schema enforcement — every field read as `StringType` so type-mismatched values survive parsing instead of silently vanishing, and a `_corrupt_record` column catches payloads that aren't valid JSON at all).
2. Runs explicit validation and assigns a **specific `failure_reason`** (first match wins): `malformed JSON payload`, `missing field: <name>`, `diagnosis/probe_type/severity not in allowed set`, `event_ts unparseable`. **Unchanged this week.**
3. Per micro-batch (`foreachBatch`):
   - **Valid** rows → printed to console (`.show()`) **and appended to a Delta table** at `s3a://lakehouse/respiratory/valid_events`, partitioned by `event_date` (derived from `event_ts` — a time-based partition is the standard pattern for an event/fact table and sets up cleanly for the time-travel demo below).
   - After the first successful write, the job registers the table once in Hive Metastore: `CREATE TABLE IF NOT EXISTS lakehouse.valid_events USING DELTA LOCATION 's3a://lakehouse/respiratory/valid_events'` (has to happen *after* data exists, since Delta reads the schema from the table's own transaction log).
   - **Invalid** rows → still appended to local `output/dead_letter/` as JSON, unchanged.
   - A running counter still prints after every batch.

The streaming query's checkpoint now lives on MinIO too: `s3a://lakehouse/checkpoints/stream_ingest`. The dead-letter sink's local files are untouched.

## Run steps

### 0. Prerequisites

Docker Desktop running. From this folder:

```powershell
pip install -r requirements.txt
```

The Hive Metastore container needs a Postgres JDBC driver jar that isn't bundled in its image. Download it once:

```powershell
New-Item -ItemType Directory -Force drivers | Out-Null
Invoke-WebRequest -Uri "https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.13/postgresql-42.7.13.jar" -OutFile "drivers/postgresql-42.7.13.jar"
```

### (a) Start the stack

```powershell
docker compose up -d
docker compose ps        # wait until kafka, minio, postgres, hive-metastore all show "healthy"
```

First run pulls several large images (MinIO, Spark, Hive) — can take a few minutes.

(Optional) explicitly create the Kafka topic instead of relying on auto-create:
```powershell
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --create --if-not-exists `
  --topic respiratory_events --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
```

MinIO console (to browse the `lakehouse` bucket visually): http://localhost:9001 — login `minioadmin` / `minioadmin123`.

### (b) Run the producer (on your host) — unchanged

```powershell
python producer.py --rate 5 --malformed-rate 0.05
```

Leave running in its own terminal.

### (c) Run the streaming job (inside the Spark container)

In a second terminal:

```powershell
docker compose exec spark spark-submit `
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.6,io.delta:delta-spark_2.12:3.3.3,org.apache.hadoop:hadoop-aws:3.3.4 `
  stream_ingest.py
```

First run downloads these packages **and** the auto-resolved Hive 3.1.3 metastore client via Maven (needs internet the first time; cached afterward in the `spark_ivy_cache` Docker volume, which *does* persist across `docker compose down` — unlike Week 1's ephemeral ivy cache).

### (d) Watch it work

- Console still prints valid rows, dead-lettered rows with their `failure_reason`, and the running totals.
- Dead-letter output: `output/dead_letter/*.json` — same as Week 1.
- List the Delta files landing in MinIO (using the `mc` image directly on the compose network — note the `--entrypoint sh` override, since the image's default entrypoint is `mc` itself):
  ```powershell
  docker run --rm --network lakehouse-net --entrypoint sh quay.io/minio/mc:RELEASE.2025-08-13T08-35-41Z `
    -c "mc alias set local http://minio:9000 minioadmin minioadmin123 >/dev/null && mc ls --recursive local/lakehouse/respiratory/valid_events"
  ```
  You should see a partitioned layout like `event_date=2026-09-20/part-....snappy.parquet` alongside a `_delta_log/` folder with one `.json` commit per micro-batch.
  (or browse visually at http://localhost:9001)
- Query the table with `spark-sql` (after the streaming job has written at least one batch):
  ```powershell
  docker compose exec spark spark-sql `
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.6,io.delta:delta-spark_2.12:3.3.3,org.apache.hadoop:hadoop-aws:3.3.4 `
    --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension `
    --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog `
    --conf spark.sql.catalogImplementation=hive `
    --conf spark.sql.hive.metastore.jars=builtin `
    --conf spark.sql.warehouse.dir=file:///warehouse `
    --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 `
    --conf spark.hadoop.fs.s3a.access.key=minioadmin `
    --conf spark.hadoop.fs.s3a.secret.key=minioadmin123 `
    --conf spark.hadoop.fs.s3a.path.style.access=true `
    -e "SELECT diagnosis, count(*) FROM lakehouse.valid_events GROUP BY diagnosis;"
  ```
- **Time travel** (after at least 2 micro-batches have landed — send a second burst with `producer.py --count N` while the job is running to force a second batch): check the table history, then query an earlier version, using the same `spark-sql` flags as above:
  ```sql
  DESCRIBE HISTORY lakehouse.valid_events;
  SELECT count(*) FROM lakehouse.valid_events VERSION AS OF 0;
  SELECT count(*) FROM lakehouse.valid_events;  -- current version, should be higher
  ```
  Verified in testing: version 0 had 90 rows, the current version (after a second batch) had 155 — `VERSION AS OF 0` correctly returned 90, proving real ACID time travel against the transaction log, not just a re-read of current state.

### Shutting down

```powershell
# Ctrl+C the producer, Ctrl+C the spark-submit job, then:
docker compose down
```

`output/` (dead-letter files) and the `minio_data` / `spark_ivy_cache` / `hive_warehouse` Docker volumes persist across `docker compose down`. `postgres` has no volume (ephemeral, see above), so the Hive Metastore catalog is rebuilt empty on every restart — but the actual Delta table data on `minio_data` survives, and `stream_ingest.py` re-registers `lakehouse.valid_events` automatically on its next run (`CREATE TABLE IF NOT EXISTS ... LOCATION`). Use `docker compose down -v` for a fully clean slate (wipes the Delta table too).
