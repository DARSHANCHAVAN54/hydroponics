# Databricks notebook source
from pyspark.sql import functions as F

BRONZE_IOT = "hydropulse.bronze.bronze_iot_telemetry"
BRONZE_MASTER = "hydropulse.bronze.bronze_farm_crop_master"

SILVER_IOT = "hydropulse.silver.silver_iot_enriched"
SILVER_INVALID = "hydropulse.silver.silver_invalid_records"

GOLD_CURRENT = "hydropulse.gold.gold_current_status"
GOLD_ALERTS = "hydropulse.gold.gold_alerts"
GOLD_DAILY = "hydropulse.gold.gold_daily_kpi"

# COMMAND ----------

tables = [
    BRONZE_IOT,
    BRONZE_MASTER,
    SILVER_IOT,
    SILVER_INVALID,
    GOLD_CURRENT,
    GOLD_ALERTS,
    GOLD_DAILY
]

for table_name in tables:
    exists = spark.catalog.tableExists(table_name)
    print(f"{table_name}: {'PASS' if exists else 'FAIL'}")

# COMMAND ----------

for table_name in tables:
    count = spark.table(table_name).count()
    print(f"{table_name}: {count}")

# COMMAND ----------

silver_duplicate_count = (
    spark.table(SILVER_IOT)
    .groupBy("event_id")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

print(
    "Silver duplicate event IDs:",
    silver_duplicate_count
)

# COMMAND ----------

required_silver_columns = [
    "event_id",
    "farm_id",
    "zone_id",
    "device_id",
    "event_timestamp",
    "crop_type",
    "ph",
    "ec_ms_cm"
]

for column_name in required_silver_columns:

    null_count = (
        spark.table(SILVER_IOT)
        .filter(F.col(column_name).isNull())
        .count()
    )

    print(
        f"{column_name}: "
        f"{'PASS' if null_count == 0 else 'FAIL'} "
        f"(nulls={null_count})"
    )

# COMMAND ----------

current_count = spark.table(GOLD_CURRENT).count()

print("Current-status rows:", current_count)

# COMMAND ----------

current_duplicates = (
    spark.table(GOLD_CURRENT)
    .groupBy("farm_id", "zone_id")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

print(
    "Duplicate current-status keys:",
    current_duplicates
)

# COMMAND ----------

duplicate_active_alerts = (
    spark.table(GOLD_ALERTS)
    .filter(F.col("alert_status") == "ACTIVE")
    .groupBy("alert_key")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

print(
    "Duplicate active alerts:",
    duplicate_active_alerts
)

# COMMAND ----------

display(
    spark.table(GOLD_ALERTS)
    .groupBy(
        "alert_status",
        "severity"
    )
    .count()
)

# COMMAND ----------

daily_duplicates = (
    spark.table(GOLD_DAILY)
    .groupBy(
        "kpi_date",
        "farm_id",
        "zone_id"
    )
    .count()
    .filter(F.col("count") > 1)
    .count()
)

print(
    "Duplicate daily KPI keys:",
    daily_duplicates
)

# COMMAND ----------

invalid_health_scores = (
    spark.table(GOLD_DAILY)
    .filter(
        (F.col("health_score") < 0)
        | (F.col("health_score") > 100)
        | F.col("health_score").isNull()
    )
    .count()
)

print(
    "Invalid health scores:",
    invalid_health_scores
)

# COMMAND ----------

invalid_anomaly_rates = (
    spark.table(GOLD_DAILY)
    .filter(
        (F.col("anomaly_rate_pct") < 0)
        | (F.col("anomaly_rate_pct") > 100)
    )
    .count()
)

print(
    "Invalid anomaly rates:",
    invalid_anomaly_rates
)


# COMMAND ----------

df_silver_latest = (
    spark.table(SILVER_IOT)
    .groupBy(
        "farm_id",
        "zone_id"
    )
    .agg(
        F.max("event_timestamp").alias(
            "silver_latest_timestamp"
        )
    )
)

# COMMAND ----------

df_freshness_check = (
    spark.table(GOLD_CURRENT)
    .select(
        "farm_id",
        "zone_id",
        "last_reading_at"
    )
    .join(
        df_silver_latest,
        on=["farm_id", "zone_id"],
        how="left"
    )
    .withColumn(
        "timestamp_match",
        F.col("last_reading_at")
        == F.col("silver_latest_timestamp")
    )
)

# COMMAND ----------

freshness_failures = (
    df_freshness_check
    .filter(~F.col("timestamp_match"))
    .count()
)

print(
    "Gold freshness failures:",
    freshness_failures
)


# COMMAND ----------

checks = {
    "Silver duplicate event IDs": silver_duplicate_count == 0,
    "Gold current duplicate keys": current_duplicates == 0,
    "Gold active alert duplicates": duplicate_active_alerts == 0,
    "Gold daily duplicate keys": daily_duplicates == 0,
    "Invalid health scores": invalid_health_scores == 0,
    "Invalid anomaly rates": invalid_anomaly_rates == 0,
    "Gold freshness": freshness_failures == 0
}

print("=" * 55)
print("HYDROPULSE PIPELINE VALIDATION")
print("=" * 55)

for check_name, passed in checks.items():

    status = "PASS" if passed else "FAIL"

    print(f"{status:5} | {check_name}")

print("=" * 55)

if all(checks.values()):
    print("OVERALL STATUS: PASS")
else:
    print("OVERALL STATUS: FAIL")