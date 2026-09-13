# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable

# COMMAND ----------

SILVER_IOT_TABLE = "hydropulse.silver.silver_iot_enriched"
GOLD_CURRENT_TABLE = "hydropulse.gold.gold_current_status"

# COMMAND ----------

df_silver = spark.table(SILVER_IOT_TABLE)

print("Silver records:", df_silver.count())

# COMMAND ----------

latest_window = (
    Window
    .partitionBy(
        "farm_id",
        "zone_id"
    )
    .orderBy(
        F.col("event_timestamp").desc(),
        F.col("eventhub_enqueued_time").desc(),
        F.col("eventhub_offset").desc()
    )
)

# COMMAND ----------

df_ranked = (
    df_silver
    .withColumn(
        "latest_rank",
        F.row_number().over(latest_window)
    )
)

# COMMAND ----------

df_latest = (
    df_ranked
    .filter(F.col("latest_rank") == 1)
    .drop("latest_rank")
)

# COMMAND ----------

print("Current farm-zone states:", df_latest.count())

# COMMAND ----------

df_gold_current = (
    df_latest
    .select(
        # Business dimensions
        "farm_id",
        "zone_id",
        "crop_type",
        "location",
        "state",

        # Latest device
        "device_id",

        # Current sensor values
        "ph",
        "ec_ms_cm",
        "tds_ppm",
        "water_temp_c",
        "water_level_cm",
        "dissolved_oxygen_mg_l",
        "ambient_temp_c",
        "humidity_pct",
        "co2_ppm",

        # Crop-specific thresholds
        "ph_min",
        "ph_max",
        "ec_min_ms_cm",
        "ec_max_ms_cm",
        "water_temp_min_c",
        "water_temp_max_c",
        "humidity_min_pct",
        "humidity_max_pct",
        "do_min_mg_l",
        "water_level_min_cm",
        "water_level_max_cm",

        # Sensor statuses
        "ph_status",
        "ec_status",
        "water_temp_status",
        "water_level_status",
        "humidity_status",
        "do_status",

        # Operational summary
        "is_anomaly",
        "anomaly_count",
        "severity",
        "overall_status",

        # Controller
        "controller_mode",
        "is_ph_reducer_on",
        "is_ph_increaser_on",
        "is_water_refill_on",
        "is_nutrient_pump_on",
        "is_humidifier_on",
        "is_exhaust_fan_on",
        "is_aerator_on",

        # Device/network
        "battery_voltage",
        "has_sequence_gap",
        "missing_packet_count",

        F.col("event_timestamp").alias("last_reading_at")
    )
    .withColumn(
        "gold_updated_at",
        F.current_timestamp()
    )
)

# COMMAND ----------

duplicate_gold_keys = (
    df_gold_current
    .groupBy(
        "farm_id",
        "zone_id"
    )
    .count()
    .filter(F.col("count") > 1)
    .count()
)

print(
    "Duplicate farm-zone keys:",
    duplicate_gold_keys
)

# COMMAND ----------

print(
    "Gold current-status rows:",
    df_gold_current.count()
)

# COMMAND ----------

display(
    df_gold_current
    .select(
        "farm_id",
        "zone_id",
        "crop_type",
        "location",
        "ph",
        "ec_ms_cm",
        "water_temp_c",
        "water_level_cm",
        "dissolved_oxygen_mg_l",
        "humidity_pct",
        "is_anomaly",
        "severity",
        "overall_status",
        "last_reading_at"
    )
    .orderBy(
        "farm_id",
        "zone_id"
    )
)

# COMMAND ----------

if not spark.catalog.tableExists(GOLD_CURRENT_TABLE):

    (
        df_gold_current
        .write
        .format("delta")
        .saveAsTable(GOLD_CURRENT_TABLE)
    )

    print(f"Created {GOLD_CURRENT_TABLE}")

else:

    gold_target = DeltaTable.forName(
        spark,
        GOLD_CURRENT_TABLE
    )

    (
        gold_target.alias("target")
        .merge(
            df_gold_current.alias("source"),
            """
            target.farm_id = source.farm_id
            AND target.zone_id = source.zone_id
            """
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )

    print(f"Updated {GOLD_CURRENT_TABLE}")

# COMMAND ----------

df_gold_check = spark.table(GOLD_CURRENT_TABLE)

print(
    "Gold current-status rows:",
    df_gold_check.count()
)

# COMMAND ----------

duplicate_count = (
    df_gold_check
    .groupBy("farm_id", "zone_id")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

print(
    "Duplicate farm-zone keys:",
    duplicate_count
)

# COMMAND ----------

display(
    df_gold_check
    .groupBy(
        "overall_status"
    )
    .count()
)

# COMMAND ----------

display(
    df_gold_check
    .filter(F.col("is_anomaly"))
    .select(
        "farm_id",
        "zone_id",
        "crop_type",
        "severity",
        "overall_status",
        "ph_status",
        "ec_status",
        "water_temp_status",
        "water_level_status",
        "humidity_status",
        "do_status",
        "last_reading_at"
    )
)