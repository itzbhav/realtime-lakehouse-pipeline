"""
stream_ingest.py — PySpark Structured Streaming job.

Reads raw JSON respiratory events from Kafka topic `respiratory_events`,
applies an explicit schema, validates each record against business rules,
and routes:
  - valid records  -> console + Delta table on MinIO (s3a://lakehouse/...),
                       partitioned by event_date, registered in Hive
                       Metastore as lakehouse.valid_events
  - invalid records -> local dead-letter JSON folder, tagged with a
                        specific failure_reason (not just "invalid")
                        (unchanged from Week 1)

Run inside the `spark` container, e.g.:
  docker compose exec spark spark-submit \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.6,io.delta:delta-spark_2.12:3.3.3,org.apache.hadoop:hadoop-aws:3.3.4 \
    stream_ingest.py
"""

import argparse
import os

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StructType, StructField, StringType

DIAGNOSES = ["COPD", "Asthma", "Pneumonia", "Bronchitis", "COVID-19", "Normal"]
PROBE_TYPES = ["SpO2", "Spirometer", "Capnograph", "Nasal Cannula"]
SEVERITIES = ["Low", "Medium", "High", "Critical"]

TS_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"

EVENT_SCHEMA = StructType(
    [
        StructField("event_id", StringType(), True),
        StructField("patient_id", StringType(), True),
        StructField("diagnosis", StringType(), True),
        StructField("probe_type", StringType(), True),
        StructField("severity", StringType(), True),
        StructField("event_ts", StringType(), True),
        StructField("_corrupt_record", StringType(), True),
    ]
)


class RunningState:
    def __init__(self):
        self.valid = 0
        self.dead_lettered = 0
        self.table_registered = False


def build_failure_reason_column():
    return (
        F.when(F.col("_corrupt_record").isNotNull(), F.lit("malformed JSON payload"))
        .when(F.col("event_id").isNull(), F.lit("missing field: event_id"))
        .when(F.col("patient_id").isNull(), F.lit("missing field: patient_id"))
        .when(F.col("diagnosis").isNull(), F.lit("missing field: diagnosis"))
        .when(F.col("probe_type").isNull(), F.lit("missing field: probe_type"))
        .when(F.col("severity").isNull(), F.lit("missing field: severity"))
        .when(F.col("event_ts").isNull(), F.lit("missing field: event_ts"))
        .when(~F.col("diagnosis").isin(DIAGNOSES), F.lit("diagnosis not in allowed set"))
        .when(~F.col("probe_type").isin(PROBE_TYPES), F.lit("probe_type not in allowed set"))
        .when(~F.col("severity").isin(SEVERITIES), F.lit("severity not in allowed set"))
        .when(F.to_timestamp(F.col("event_ts"), TS_FORMAT).isNull(), F.lit("event_ts unparseable"))
        .otherwise(F.lit(None).cast(StringType()))
    )


def make_batch_writer(spark, valid_delta_path: str, dead_path: str, database: str, table: str, state: RunningState):
    def process_batch(batch_df, batch_id: int):
        batch_df = batch_df.cache()

        valid_df = (
            batch_df.filter(F.col("failure_reason").isNull())
            .withColumn("event_ts", F.to_timestamp(F.col("event_ts"), TS_FORMAT))
            .withColumn("event_date", F.to_date(F.col("event_ts")))
            .withColumn("ingested_at", F.current_timestamp())
            .select("event_id", "patient_id", "diagnosis", "probe_type", "severity", "event_ts", "event_date", "ingested_at")
        )

        dead_df = (
            batch_df.filter(F.col("failure_reason").isNotNull())
            .withColumn("dead_lettered_at", F.current_timestamp())
            .select("raw_value", "failure_reason", "dead_lettered_at")
        )

        valid_count = valid_df.count()
        dead_count = dead_df.count()

        if valid_count > 0:
            valid_df.write.format("delta").mode("append").partitionBy("event_date").save(valid_delta_path)
            print(f"\n===== batch {batch_id}: {valid_count} VALID record(s) -> Delta ({valid_delta_path}) =====")
            valid_df.show(20, truncate=False)

            if not state.table_registered:
                spark.sql(
                    f"CREATE TABLE IF NOT EXISTS {database}.{table} USING DELTA LOCATION '{valid_delta_path}'"
                )
                state.table_registered = True
                print(f">>> Registered Delta table {database}.{table} in Hive Metastore -> {valid_delta_path}")

        if dead_count > 0:
            dead_df.write.mode("append").json(dead_path)
            print(f"\n----- batch {batch_id}: {dead_count} DEAD-LETTERED record(s) -----")
            dead_df.show(20, truncate=False)

        state.valid += valid_count
        state.dead_lettered += dead_count
        total = state.valid + state.dead_lettered
        rate = (state.dead_lettered / total * 100) if total else 0.0
        print(
            f">>> RUNNING TOTALS — valid: {state.valid} | dead-lettered: {state.dead_lettered} "
            f"| dead-letter rate: {rate:.1f}%\n"
        )

        batch_df.unpersist()

    return process_batch


def build_spark_session(args):
    builder = (
        SparkSession.builder.appName("respiratory-stream-ingest")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.warehouse.dir", "file:///warehouse")
        .config("spark.hadoop.fs.s3a.endpoint", args.s3_endpoint)
        .config("spark.hadoop.fs.s3a.access.key", args.s3_access_key)
        .config("spark.hadoop.fs.s3a.secret.key", args.s3_secret_key)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.sql.catalogImplementation", "hive")
        .config("spark.sql.hive.metastore.jars", "builtin")
        .enableHiveSupport()
    )
    return builder.getOrCreate()


def main():
    parser = argparse.ArgumentParser(description="Respiratory event stream ingest + validation")
    parser.add_argument("--bootstrap", default="kafka:19092", help="Kafka bootstrap servers (internal listener)")
    parser.add_argument("--topic", default="respiratory_events", help="source Kafka topic")
    parser.add_argument("--starting-offsets", default="earliest", choices=["earliest", "latest"])
    parser.add_argument("--valid-delta-path", default="s3a://lakehouse/respiratory/valid_events")
    parser.add_argument("--dead-path", default="output/dead_letter")
    parser.add_argument("--checkpoint-path", default="s3a://lakehouse/checkpoints/stream_ingest")
    parser.add_argument("--database", default="lakehouse")
    parser.add_argument("--table", default="valid_events")
    parser.add_argument("--trigger-seconds", type=int, default=5)
    parser.add_argument("--s3-endpoint", default="http://minio:9000")
    parser.add_argument("--s3-access-key", default=os.environ.get("MINIO_ACCESS_KEY", "minioadmin"))
    parser.add_argument("--s3-secret-key", default=os.environ.get("MINIO_SECRET_KEY", "minioadmin123"))
    args = parser.parse_args()

    spark = build_spark_session(args)
    spark.sparkContext.setLogLevel("WARN")

    spark.sql(f"CREATE DATABASE IF NOT EXISTS {args.database}")

    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", args.bootstrap)
        .option("subscribe", args.topic)
        .option("startingOffsets", args.starting_offsets)
        .option("failOnDataLoss", "false")
        .load()
    )

    with_raw_value = raw.select(F.col("value").cast("string").alias("raw_value"))

    parsed = with_raw_value.select(
        "raw_value",
        F.from_json(
            F.col("raw_value"),
            EVENT_SCHEMA,
            {"mode": "PERMISSIVE", "columnNameOfCorruptRecord": "_corrupt_record"},
        ).alias("data"),
    ).select("raw_value", "data.*")

    classified = parsed.withColumn("failure_reason", build_failure_reason_column())

    state = RunningState()
    query = (
        classified.writeStream.foreachBatch(
            make_batch_writer(spark, args.valid_delta_path, args.dead_path, args.database, args.table, state)
        )
        .option("checkpointLocation", args.checkpoint_path)
        .trigger(processingTime=f"{args.trigger_seconds} seconds")
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()
