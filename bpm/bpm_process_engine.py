"""
bpm_process_engine.py - PySpark Structured Streaming BPM process-mining engine
for the quick-commerce order fulfillment case study.

Reads case events (case_id, activity, timestamp, resource, location, metadata)
from Kafka topic `order_fulfillment_events` and, per micro-batch:

  1. Reconstructs each case's process state (in-batch via window functions,
     across batches via a persisted `case_state` Delta table), so multi-event
     cases are handled correctly even when several of a case's events land
     in the same micro-batch.
  2. Flags four kinds of process anomalies on every event:
       - is_duplicate       : same (case_id, activity) observed twice
       - is_out_of_sequence : activity arrived out of the expected stage order
       - sla_breached       : this stage-to-stage gap exceeded its SLA
       - total_sla_breached : (at DOORSTEP_DELIVERED) total case time exceeded
                               metadata.target_total_minutes
  3. Writes every event, enriched with the above, to a Delta table
     `bpm.process_events` (the immutable audit / fact log) and upserts the
     latest per-case snapshot into `bpm.case_state` (a Delta MERGE).
  4. Registers both tables in Hive Metastore (once) and prints a running
     KPI summary each batch: SLA breach rate, duplicate/out-of-sequence
     counts, per-stage average duration (bottleneck signal), and throughput.

Run inside the `spark` container:
  docker compose exec spark spark-submit \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.6,io.delta:delta-spark_2.12:3.3.3,org.apache.hadoop:hadoop-aws:3.3.4 \
    bpm/bpm_process_engine.py
"""

import argparse
import os

from pyspark.sql import SparkSession, Window, functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, TimestampType, DoubleType
)

STAGE_INDEX = {
    "ORDER_RECEIVED": 0,
    "PICKER_ASSIGNED": 1,
    "ITEMS_BAGGED": 2,
    "RIDER_ASSIGNED": 3,
    "DISPATCHED_FROM_DARKSTORE": 4,
    "DOORSTEP_DELIVERED": 5,
}

# None => data-driven from metadata.picking_sla_minutes for that specific transition;
# the assignment's example payload only ships a picking-stage SLA, so the other
# stage targets are fixed ops constants (documented in bpm/README.md).
STAGE_SLA_MINUTES = {
    ("ORDER_RECEIVED", "PICKER_ASSIGNED"): 1.0,
    ("PICKER_ASSIGNED", "ITEMS_BAGGED"): None,
    ("ITEMS_BAGGED", "RIDER_ASSIGNED"): 2.0,
    ("RIDER_ASSIGNED", "DISPATCHED_FROM_DARKSTORE"): 2.0,
    ("DISPATCHED_FROM_DARKSTORE", "DOORSTEP_DELIVERED"): 7.0,
}

EVENT_SCHEMA = StructType([
    StructField("case_id", StringType(), True),
    StructField("activity", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("resource", StringType(), True),
    StructField("location", StringType(), True),
    StructField("metadata", StructType([
        StructField("item_count", IntegerType(), True),
        StructField("target_total_minutes", IntegerType(), True),
        StructField("picking_sla_minutes", IntegerType(), True),
        StructField("substituted_item", StringType(), True),
        StructField("reason", StringType(), True),
    ]), True),
])

STATE_SCHEMA = StructType([
    StructField("case_id", StringType(), False),
    StructField("last_activity", StringType(), True),
    StructField("last_activity_ts", TimestampType(), True),
    StructField("last_stage_index", IntegerType(), True),
    StructField("order_received_ts", TimestampType(), True),
    StructField("item_count", IntegerType(), True),
    StructField("target_total_minutes", IntegerType(), True),
    StructField("picking_sla_minutes", IntegerType(), True),
    StructField("updated_at", TimestampType(), True),
])


def stage_index_col(activity_col):
    expr = F.lit(None).cast("int")
    for name, idx in STAGE_INDEX.items():
        expr = F.when(activity_col == name, F.lit(idx)).otherwise(expr)
    return expr


def sla_limit_minutes_col(prev_activity_col, activity_col, picking_sla_col):
    expr = F.lit(None).cast("double")
    for (prev, cur), minutes in STAGE_SLA_MINUTES.items():
        cond = (prev_activity_col == prev) & (activity_col == cur)
        value = picking_sla_col.cast("double") if minutes is None else F.lit(minutes)
        expr = F.when(cond, value).otherwise(expr)
    return expr


class RunningTotals:
    def __init__(self):
        self.events = 0
        self.cases_delivered = 0
        self.sla_breaches = 0
        self.duplicates = 0
        self.out_of_sequence = 0
        self.substitutions = 0
        self.process_registered = False
        self.state_registered = False


def load_state(spark, path):
    try:
        return spark.read.format("delta").load(path)
    except Exception:
        return spark.createDataFrame([], STATE_SCHEMA)


def make_batch_processor(spark, process_events_path, case_state_path, database, totals: RunningTotals):
    def process_batch(batch_df, batch_id: int):
        batch_df = (
            batch_df.withColumn("event_ts", F.to_timestamp("timestamp"))
            .withColumn("stage_index", stage_index_col(F.col("activity")))
            .cache()
        )

        sub_df = batch_df.filter(F.col("activity") == "ITEM_SUBSTITUTED")
        main_df = batch_df.filter(F.col("activity") != "ITEM_SUBSTITUTED")

        if main_df.rdd.isEmpty() and sub_df.rdd.isEmpty():
            batch_df.unpersist()
            return

        state_df = load_state(spark, case_state_path).select(
            F.col("case_id").alias("st_case_id"),
            F.col("last_activity").alias("st_last_activity"),
            F.col("last_activity_ts").alias("st_last_activity_ts"),
            F.col("last_stage_index").alias("st_last_stage_index"),
            F.col("order_received_ts").alias("st_order_received_ts"),
        )

        w = Window.partitionBy("case_id").orderBy("event_ts")
        main_df = (
            main_df.withColumn("in_batch_prev_activity", F.lag("activity").over(w))
            .withColumn("in_batch_prev_ts", F.lag("event_ts").over(w))
            .withColumn("in_batch_prev_idx", F.lag("stage_index").over(w))
            .withColumn(
                "in_batch_order_ts",
                F.min(F.when(F.col("activity") == "ORDER_RECEIVED", F.col("event_ts"))).over(
                    Window.partitionBy("case_id")
                ),
            )
        )

        joined = main_df.join(state_df, main_df.case_id == state_df.st_case_id, "left")

        enriched = (
            joined.withColumn(
                "prev_activity", F.coalesce(F.col("in_batch_prev_activity"), F.col("st_last_activity"))
            )
            .withColumn("prev_ts", F.coalesce(F.col("in_batch_prev_ts"), F.col("st_last_activity_ts")))
            .withColumn("prev_stage_index", F.coalesce(F.col("in_batch_prev_idx"), F.col("st_last_stage_index")))
            .withColumn(
                "order_received_ts", F.coalesce(F.col("in_batch_order_ts"), F.col("st_order_received_ts"))
            )
            .withColumn(
                "stage_minutes",
                F.when(
                    F.col("prev_ts").isNotNull(),
                    (F.col("event_ts").cast("long") - F.col("prev_ts").cast("long")) / 60.0,
                ),
            )
            .withColumn(
                "is_duplicate",
                (F.col("activity") == F.col("prev_activity")).cast("boolean"),
            )
            .withColumn(
                "is_out_of_sequence",
                F.when(F.col("prev_stage_index").isNull(), F.col("stage_index") != 0)
                .when(F.col("activity") == F.col("prev_activity"), F.lit(False))
                .otherwise(F.col("stage_index") <= F.col("prev_stage_index")),
            )
            .withColumn(
                "sla_limit_minutes",
                sla_limit_minutes_col(
                    F.col("prev_activity"), F.col("activity"), F.col("metadata.picking_sla_minutes")
                ),
            )
            .withColumn(
                "sla_breached",
                F.when(
                    F.col("sla_limit_minutes").isNotNull() & F.col("stage_minutes").isNotNull(),
                    F.col("stage_minutes") > F.col("sla_limit_minutes"),
                ).otherwise(F.lit(False)),
            )
            .withColumn(
                "total_minutes",
                F.when(
                    F.col("order_received_ts").isNotNull(),
                    (F.col("event_ts").cast("long") - F.col("order_received_ts").cast("long")) / 60.0,
                ),
            )
            .withColumn(
                "total_sla_breached",
                (F.col("activity") == "DOORSTEP_DELIVERED")
                & F.col("total_minutes").isNotNull()
                & (F.col("total_minutes") > F.col("metadata.target_total_minutes")),
            )
        )

        sub_enriched = (
            sub_df.withColumn("prev_activity", F.lit(None).cast("string"))
            .withColumn("stage_minutes", F.lit(None).cast("double"))
            .withColumn("is_duplicate", F.lit(False))
            .withColumn("is_out_of_sequence", F.lit(False))
            .withColumn("sla_limit_minutes", F.lit(None).cast("double"))
            .withColumn("sla_breached", F.lit(False))
            .withColumn("total_minutes", F.lit(None).cast("double"))
            .withColumn("total_sla_breached", F.lit(False))
        )

        fact_cols = [
            "case_id", "activity", "event_ts", "resource", "location", "metadata",
            "prev_activity", "stage_minutes", "is_duplicate", "is_out_of_sequence",
            "sla_limit_minutes", "sla_breached", "total_minutes", "total_sla_breached",
        ]
        fact_df = enriched.select(*fact_cols).unionByName(sub_enriched.select(*fact_cols))
        fact_df = fact_df.withColumn("event_date", F.to_date("event_ts")).cache()

        fact_count = fact_df.count()
        if fact_count > 0:
            fact_df.write.format("delta").mode("append").partitionBy("event_date").save(process_events_path)
            if not totals.process_registered:
                spark.sql(f"CREATE DATABASE IF NOT EXISTS {database}")
                spark.sql(
                    f"CREATE TABLE IF NOT EXISTS {database}.process_events USING DELTA LOCATION '{process_events_path}'"
                )
                totals.process_registered = True

        # latest observed state per case in this batch (audit log keeps every event;
        # this projection is just "what we last saw", warts and all)
        latest_w = Window.partitionBy("case_id").orderBy(F.col("event_ts").desc())
        new_state = (
            enriched.withColumn("rn", F.row_number().over(latest_w))
            .filter(F.col("rn") == 1)
            .select(
                "case_id",
                F.col("activity").alias("last_activity"),
                F.col("event_ts").alias("last_activity_ts"),
                F.col("stage_index").alias("last_stage_index"),
                F.col("order_received_ts"),
                F.col("metadata.item_count").alias("item_count"),
                F.col("metadata.target_total_minutes").alias("target_total_minutes"),
                F.col("metadata.picking_sla_minutes").alias("picking_sla_minutes"),
                F.current_timestamp().alias("updated_at"),
            )
        )

        if new_state.rdd.isEmpty():
            pass
        elif totals.state_registered or _delta_table_exists(spark, case_state_path):
            from delta.tables import DeltaTable

            target = DeltaTable.forPath(spark, case_state_path)
            (
                target.alias("t")
                .merge(new_state.alias("s"), "t.case_id = s.case_id")
                .whenMatchedUpdateAll()
                .whenNotMatchedInsertAll()
                .execute()
            )
        else:
            new_state.write.format("delta").mode("overwrite").save(case_state_path)

        if not totals.state_registered and not new_state.rdd.isEmpty():
            spark.sql(f"CREATE DATABASE IF NOT EXISTS {database}")
            spark.sql(
                f"CREATE TABLE IF NOT EXISTS {database}.case_state USING DELTA LOCATION '{case_state_path}'"
            )
            totals.state_registered = True

        # ---- console KPI report for this batch ----
        sla_breach_count = enriched.filter(F.col("sla_breached") | F.col("total_sla_breached")).count()
        dup_count = enriched.filter(F.col("is_duplicate")).count()
        oos_count = enriched.filter(F.col("is_out_of_sequence")).count()
        sub_count = sub_df.count()
        delivered_count = enriched.filter(F.col("activity") == "DOORSTEP_DELIVERED").count()

        totals.events += fact_count
        totals.cases_delivered += delivered_count
        totals.sla_breaches += sla_breach_count
        totals.duplicates += dup_count
        totals.out_of_sequence += oos_count
        totals.substitutions += sub_count

        print(f"\n===== batch {batch_id}: {fact_count} event(s) =====")
        if fact_count > 0:
            enriched.select(
                "case_id", "activity", "prev_activity", F.round("stage_minutes", 2).alias("stage_minutes"),
                "sla_limit_minutes", "sla_breached", "is_duplicate", "is_out_of_sequence", "total_sla_breached",
            ).show(20, truncate=False)

        bottleneck = (
            enriched.filter(F.col("stage_minutes").isNotNull())
            .groupBy("prev_activity", "activity")
            .agg(F.round(F.avg("stage_minutes"), 2).alias("avg_minutes"), F.count("*").alias("n"))
            .orderBy(F.desc("avg_minutes"))
        )
        if bottleneck.count() > 0:
            print("---- stage durations this batch (bottleneck candidates) ----")
            bottleneck.show(10, truncate=False)

        print(
            f">>> batch KPIs — sla_breaches: {sla_breach_count} | duplicates: {dup_count} | "
            f"out_of_sequence: {oos_count} | substitutions: {sub_count} | delivered (throughput): {delivered_count}"
        )
        print(
            f">>> RUNNING TOTALS — events: {totals.events} | delivered: {totals.cases_delivered} | "
            f"sla_breaches: {totals.sla_breaches} | duplicates: {totals.duplicates} | "
            f"out_of_sequence: {totals.out_of_sequence} | substitutions: {totals.substitutions}\n"
        )

        batch_df.unpersist()
        fact_df.unpersist()

    return process_batch


def _delta_table_exists(spark, path):
    try:
        from delta.tables import DeltaTable

        return DeltaTable.isDeltaTable(spark, path)
    except Exception:
        return False


def build_spark_session(args):
    return (
        SparkSession.builder.appName("bpm-order-fulfillment-engine")
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
        .getOrCreate()
    )


def main():
    parser = argparse.ArgumentParser(description="BPM order-fulfillment SLA + anomaly detection engine")
    parser.add_argument("--bootstrap", default="kafka:19092")
    parser.add_argument("--topic", default="order_fulfillment_events")
    parser.add_argument("--starting-offsets", default="earliest", choices=["earliest", "latest"])
    parser.add_argument("--process-events-path", default="s3a://lakehouse/bpm/process_events")
    parser.add_argument("--case-state-path", default="s3a://lakehouse/bpm/case_state")
    parser.add_argument("--checkpoint-path", default="s3a://lakehouse/checkpoints/bpm_process_engine")
    parser.add_argument("--database", default="bpm")
    parser.add_argument("--trigger-seconds", type=int, default=10)
    parser.add_argument("--s3-endpoint", default="http://minio:9000")
    parser.add_argument("--s3-access-key", default=os.environ.get("MINIO_ACCESS_KEY", "minioadmin"))
    parser.add_argument("--s3-secret-key", default=os.environ.get("MINIO_SECRET_KEY", "minioadmin123"))
    args = parser.parse_args()

    spark = build_spark_session(args)
    spark.sparkContext.setLogLevel("WARN")

    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", args.bootstrap)
        .option("subscribe", args.topic)
        .option("startingOffsets", args.starting_offsets)
        .option("failOnDataLoss", "false")
        .load()
    )

    parsed = raw.select(
        F.from_json(F.col("value").cast("string"), EVENT_SCHEMA).alias("data")
    ).select("data.*")

    totals = RunningTotals()
    query = (
        parsed.writeStream.foreachBatch(
            make_batch_processor(spark, args.process_events_path, args.case_state_path, args.database, totals)
        )
        .option("checkpointLocation", args.checkpoint_path)
        .trigger(processingTime=f"{args.trigger_seconds} seconds")
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()
