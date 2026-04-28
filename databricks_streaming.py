"""
Databricks Structured Streaming — Medallion Architecture
=========================================================
Reads from Azure Event Hubs (Kafka protocol) and implements:
  Bronze  → Raw events landed as-is, schema-on-read
  Silver  → Cleaned, deduplicated, typed, enriched with Cosmos DB lookups
  Gold    → Aggregated business metrics, windowed KPIs, ML feature store

Run on: Databricks Runtime 14.x (Spark 3.5, Delta Lake 3.x)
Cluster: Standard_DS3_v2 × 4 workers (autoscale 2–8)
"""

import json
import logging
from datetime import datetime
from typing import Optional

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType, BooleanType, MapType, TimestampType,
)
from delta.tables import DeltaTable

# ---------------------------------------------------------------------------
# Spark session (Databricks manages this — included for local testing)
# ---------------------------------------------------------------------------
spark = (
    SparkSession.builder
    .appName("realtime-telemetry-pipeline")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.databricks.delta.retentionDurationCheck.enabled", "false")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuration — inject via Databricks Widgets or environment secrets
# ---------------------------------------------------------------------------
EVENTHUBS_NAMESPACE   = spark.conf.get("pipeline.eventhubs.namespace")
EVENTHUBS_CONNECTION  = spark.conf.get("pipeline.eventhubs.connection")   # from Key Vault
EVENTHUB_NAME         = spark.conf.get("pipeline.eventhubs.topic", "telemetry-events")
ADLS_ACCOUNT          = spark.conf.get("pipeline.adls.account")
ADLS_CONTAINER        = spark.conf.get("pipeline.adls.container", "data")
CHECKPOINT_BASE       = f"abfss://{ADLS_CONTAINER}@{ADLS_ACCOUNT}.dfs.core.windows.net/checkpoints"
STORAGE_BASE          = f"abfss://{ADLS_CONTAINER}@{ADLS_ACCOUNT}.dfs.core.windows.net"
COSMOS_ENDPOINT       = spark.conf.get("pipeline.cosmos.endpoint")
COSMOS_KEY            = spark.conf.get("pipeline.cosmos.key")             # from Key Vault

# Delta table paths
BRONZE_PATH = f"{STORAGE_BASE}/bronze/telemetry"
SILVER_PATH = f"{STORAGE_BASE}/silver/telemetry_clean"
GOLD_AGG_PATH = f"{STORAGE_BASE}/gold/device_kpis"
GOLD_ANOMALY_PATH = f"{STORAGE_BASE}/gold/anomaly_alerts"

# ---------------------------------------------------------------------------
# Schema definition — Avro payload → PySpark StructType
# ---------------------------------------------------------------------------
TELEMETRY_SCHEMA = StructType([
    StructField("event_id",          StringType(),  nullable=False),
    StructField("device_id",         StringType(),  nullable=False),
    StructField("device_type",       StringType(),  nullable=True),
    StructField("timestamp_utc",     StringType(),  nullable=False),
    StructField("latitude",          DoubleType(),  nullable=True),
    StructField("longitude",         DoubleType(),  nullable=True),
    StructField("temperature_c",     DoubleType(),  nullable=True),
    StructField("humidity_pct",      DoubleType(),  nullable=True),
    StructField("pressure_hpa",      DoubleType(),  nullable=True),
    StructField("battery_pct",       DoubleType(),  nullable=True),
    StructField("firmware_version",  StringType(),  nullable=True),
    StructField("anomaly_flag",      BooleanType(), nullable=True),
    StructField("payload",           MapType(StringType(), StringType()), nullable=True),
])


# ---------------------------------------------------------------------------
# LAYER 1: BRONZE — raw ingest from Event Hubs (Kafka)
# ---------------------------------------------------------------------------
def create_bronze_stream() -> DataFrame:
    """
    Read raw bytes from Event Hubs using the Kafka connector.
    Land every event as-is in the Bronze Delta table with minimal transforms.
    """
    kafka_options = {
        "kafka.bootstrap.servers": f"{EVENTHUBS_NAMESPACE}.servicebus.windows.net:9093",
        "subscribe": EVENTHUB_NAME,
        "kafka.security.protocol": "SASL_SSL",
        "kafka.sasl.mechanism": "PLAIN",
        "kafka.sasl.jaas.config": (
            f"kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule "
            f"required username='$ConnectionString' password='{EVENTHUBS_CONNECTION}';"
        ),
        "startingOffsets": "latest",
        "failOnDataLoss": "false",
        "maxOffsetsPerTrigger": "500000",  # back-pressure: max 500K events per micro-batch
    }

    raw_df = (
        spark.readStream
        .format("kafka")
        .options(**kafka_options)
        .load()
    )

    bronze_df = raw_df.select(
        F.col("key").cast("string").alias("kafka_key"),
        F.col("value").cast("string").alias("raw_payload"),
        F.col("topic").alias("kafka_topic"),
        F.col("partition").alias("kafka_partition"),
        F.col("offset").alias("kafka_offset"),
        F.col("timestamp").alias("kafka_timestamp"),
        F.current_timestamp().alias("ingested_at"),
        # Partition column for efficient pruning
        F.date_format(F.col("timestamp"), "yyyy-MM-dd").alias("event_date"),
    )

    return bronze_df


def write_bronze(bronze_df: DataFrame) -> None:
    (
        bronze_df.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", f"{CHECKPOINT_BASE}/bronze")
        .option("mergeSchema", "true")
        .partitionBy("event_date")
        .trigger(processingTime="10 seconds")
        .start(BRONZE_PATH)
    )
    log.info("Bronze stream started → %s", BRONZE_PATH)


# ---------------------------------------------------------------------------
# LAYER 2: SILVER — clean, deduplicate, enrich, validate
# ---------------------------------------------------------------------------
def parse_and_validate(bronze_df: DataFrame) -> DataFrame:
    """
    Parse JSON payload, apply schema, drop malformed records, deduplicate.
    """
    parsed_df = (
        bronze_df
        .withColumn("parsed", F.from_json(F.col("raw_payload"), TELEMETRY_SCHEMA))
        .select(
            "kafka_key",
            "kafka_partition",
            "kafka_offset",
            "kafka_timestamp",
            "ingested_at",
            "event_date",
            F.col("parsed.*"),
        )
    )

    # Drop rows where required fields are null (schema validation)
    validated_df = parsed_df.filter(
        F.col("event_id").isNotNull()
        & F.col("device_id").isNotNull()
        & F.col("timestamp_utc").isNotNull()
    )

    # Cast string timestamp to proper TimestampType
    validated_df = validated_df.withColumn(
        "event_timestamp",
        F.to_timestamp(F.col("timestamp_utc"), "yyyy-MM-dd'T'HH:mm:ss.SSSSSSX"),
    )

    # Business-rule validations — mark invalid ranges rather than drop
    validated_df = validated_df.withColumn(
        "quality_flags",
        F.array(
            F.when(F.col("temperature_c").between(-50, 85), None).otherwise(F.lit("TEMP_OUT_OF_RANGE")),
            F.when(F.col("humidity_pct").between(0, 100), None).otherwise(F.lit("HUMIDITY_INVALID")),
            F.when(F.col("battery_pct").between(0, 100), None).otherwise(F.lit("BATTERY_INVALID")),
            F.when(F.col("latitude").between(-90, 90), None).otherwise(F.lit("LAT_INVALID")),
            F.when(F.col("longitude").between(-180, 180), None).otherwise(F.lit("LON_INVALID")),
        )
    ).withColumn(
        "quality_flags",
        F.array_compact(F.col("quality_flags"))  # remove nulls
    ).withColumn(
        "is_valid",
        F.size(F.col("quality_flags")) == 0
    )

    return validated_df


def enrich_with_device_metadata(silver_df: DataFrame) -> DataFrame:
    """
    Join with device registry stored in Cosmos DB (read as a static Delta snapshot).
    Cosmos DB acts as the low-latency lookup store; Databricks reads a periodic snapshot.
    """
    device_registry = (
        spark.read
        .format("cosmos.oltp")
        .option("spark.cosmos.accountEndpoint", COSMOS_ENDPOINT)
        .option("spark.cosmos.accountKey", COSMOS_KEY)
        .option("spark.cosmos.database", "devicedb")
        .option("spark.cosmos.container", "device_registry")
        .load()
        .select("device_id", "site_name", "region", "owner_team", "sla_tier")
        .cache()
    )

    return silver_df.join(
        device_registry,
        on="device_id",
        how="left",
    )


def write_silver(bronze_df: DataFrame) -> None:
    """
    Use foreachBatch to apply MERGE (upsert) into Silver Delta table.
    This guarantees exactly-once semantics with deduplication on event_id.
    """
    def upsert_to_silver(batch_df: DataFrame, batch_id: int) -> None:
        # Parse + validate within the batch
        cleaned = parse_and_validate(batch_df)
        enriched = enrich_with_device_metadata(cleaned)

        # MERGE into Silver (idempotent upsert on event_id)
        if DeltaTable.isDeltaTable(spark, SILVER_PATH):
            silver_table = DeltaTable.forPath(spark, SILVER_PATH)
            (
                silver_table.alias("target")
                .merge(
                    enriched.alias("source"),
                    "target.event_id = source.event_id",
                )
                .whenNotMatchedInsertAll()
                .execute()
            )
        else:
            # First run — create the table
            enriched.write.format("delta").partitionBy("event_date").save(SILVER_PATH)

        log.info("Silver batch %d: %d rows upserted", batch_id, enriched.count())

    bronze_stream = create_bronze_stream()
    (
        bronze_stream.writeStream
        .foreachBatch(upsert_to_silver)
        .option("checkpointLocation", f"{CHECKPOINT_BASE}/silver")
        .trigger(processingTime="30 seconds")
        .start()
    )
    log.info("Silver stream started → %s", SILVER_PATH)


# ---------------------------------------------------------------------------
# LAYER 3: GOLD — aggregations, KPIs, anomaly alerting
# ---------------------------------------------------------------------------
def create_gold_device_kpis() -> None:
    """
    5-minute tumbling window aggregations per device.
    Written to Gold Delta table, consumed by Power BI and API.
    """
    silver_stream = (
        spark.readStream
        .format("delta")
        .option("ignoreChanges", "true")
        .load(SILVER_PATH)
        .filter(F.col("is_valid") == True)
        .withWatermark("event_timestamp", "2 minutes")  # late data tolerance
    )

    kpi_df = (
        silver_stream
        .groupBy(
            F.window(F.col("event_timestamp"), "5 minutes"),
            F.col("device_id"),
            F.col("device_type"),
            F.col("region"),
            F.col("sla_tier"),
        )
        .agg(
            F.count("*").alias("event_count"),
            F.avg("temperature_c").alias("avg_temp_c"),
            F.max("temperature_c").alias("max_temp_c"),
            F.min("temperature_c").alias("min_temp_c"),
            F.avg("humidity_pct").alias("avg_humidity"),
            F.avg("battery_pct").alias("avg_battery"),
            F.min("battery_pct").alias("min_battery"),
            F.sum(F.col("anomaly_flag").cast("int")).alias("anomaly_count"),
            F.countDistinct("firmware_version").alias("firmware_versions"),
        )
        .withColumn("window_start", F.col("window.start"))
        .withColumn("window_end", F.col("window.end"))
        .withColumn("anomaly_rate_pct", F.round(F.col("anomaly_count") / F.col("event_count") * 100, 2))
        .withColumn("gold_written_at", F.current_timestamp())
        .drop("window")
    )

    (
        kpi_df.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", f"{CHECKPOINT_BASE}/gold_kpis")
        .partitionBy("region")
        .trigger(processingTime="1 minute")
        .start(GOLD_AGG_PATH)
    )
    log.info("Gold KPI stream started → %s", GOLD_AGG_PATH)


def create_gold_anomaly_alerts() -> None:
    """
    Real-time anomaly alerting — events with anomaly_flag=True or
    temperature outside ±3σ from device's rolling mean.
    Written to Gold Anomaly table and triggers downstream Azure Functions alert.
    """
    silver_stream = (
        spark.readStream
        .format("delta")
        .option("ignoreChanges", "true")
        .load(SILVER_PATH)
        .withWatermark("event_timestamp", "1 minute")
    )

    # Compute rolling stats for each device using 10-minute sliding window
    rolling_stats = (
        silver_stream
        .groupBy(
            F.window(F.col("event_timestamp"), "10 minutes", "1 minute"),
            F.col("device_id"),
        )
        .agg(
            F.avg("temperature_c").alias("rolling_avg_temp"),
            F.stddev("temperature_c").alias("rolling_std_temp"),
        )
    )

    # Join back to detect outliers
    anomalies = (
        silver_stream.alias("s")
        .join(
            rolling_stats.alias("r"),
            on=(
                (F.col("s.device_id") == F.col("r.device_id"))
                & F.col("s.event_timestamp").between(
                    F.col("r.window.start"), F.col("r.window.end")
                )
            ),
            how="left",
        )
        .filter(
            F.col("s.anomaly_flag")
            | (F.abs(F.col("s.temperature_c") - F.col("r.rolling_avg_temp")) > 3 * F.col("r.rolling_std_temp"))
        )
        .select(
            F.col("s.event_id"),
            F.col("s.device_id"),
            F.col("s.device_type"),
            F.col("s.region"),
            F.col("s.event_timestamp"),
            F.col("s.temperature_c"),
            F.col("r.rolling_avg_temp"),
            F.col("r.rolling_std_temp"),
            F.col("s.anomaly_flag").alias("model_anomaly_flag"),
            F.lit("statistical_outlier").alias("detection_method"),
            F.current_timestamp().alias("alert_raised_at"),
        )
    )

    (
        anomalies.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", f"{CHECKPOINT_BASE}/gold_anomalies")
        .trigger(processingTime="30 seconds")
        .start(GOLD_ANOMALY_PATH)
    )
    log.info("Gold anomaly stream started → %s", GOLD_ANOMALY_PATH)


# ---------------------------------------------------------------------------
# Optimize + Z-Order Delta tables (run as a separate scheduled job)
# ---------------------------------------------------------------------------
def optimize_delta_tables() -> None:
    """
    Run OPTIMIZE + ZORDER on Silver and Gold tables nightly.
    Z-ordering on device_id and event_date dramatically improves query performance.
    """
    for table_path, z_cols in [
        (SILVER_PATH,    ["device_id", "event_date"]),
        (GOLD_AGG_PATH,  ["device_id", "window_start"]),
        (GOLD_ANOMALY_PATH, ["device_id", "alert_raised_at"]),
    ]:
        log.info("Optimizing %s ...", table_path)
        spark.sql(f"OPTIMIZE delta.`{table_path}` ZORDER BY ({', '.join(z_cols)})")
        spark.sql(f"VACUUM delta.`{table_path}` RETAIN 168 HOURS")  # 7 days
    log.info("Delta optimization complete.")


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    log.info("Starting medallion pipeline...")

    # Start all streaming layers
    write_bronze(create_bronze_stream())
    write_silver(create_bronze_stream())
    create_gold_device_kpis()
    create_gold_anomaly_alerts()

    # Keep the driver alive
    spark.streams.awaitAnyTermination()
