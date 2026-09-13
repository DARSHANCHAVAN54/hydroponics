# Databricks notebook source
from pyspark.sql import functions as F
from delta.tables import DeltaTable

GOLD_CURRENT_TABLE = "hydropulse.gold.gold_current_status"
GOLD_ALERTS_TABLE = "hydropulse.gold.gold_alerts"

# COMMAND ----------

df_current = spark.table(GOLD_CURRENT_TABLE)

# COMMAND ----------

df_ph_alerts = (
    df_current
    .filter(F.col("ph_status").isin("LOW", "HIGH"))
    .select(
        "farm_id",
        "zone_id",
        "crop_type",
        "location",
        "state",
        F.concat(
            F.lit("PH_"),
            F.col("ph_status")
        ).alias("alert_type"),
        F.col("ph").alias("current_value"),
        F.col("ph_min").alias("min_threshold"),
        F.col("ph_max").alias("max_threshold"),
        F.col("ph_status").alias("metric_status"),
        "severity",
        F.col("last_reading_at").alias("detected_at")
    )
)

# COMMAND ----------

df_ec_alerts = (
    df_current
    .filter(F.col("ec_status").isin("LOW", "HIGH"))
    .select(
        "farm_id",
        "zone_id",
        "crop_type",
        "location",
        "state",
        F.concat(
            F.lit("EC_"),
            F.col("ec_status")
        ).alias("alert_type"),
        F.col("ec_ms_cm").alias("current_value"),
        F.col("ec_min_ms_cm").alias("min_threshold"),
        F.col("ec_max_ms_cm").alias("max_threshold"),
        F.col("ec_status").alias("metric_status"),
        "severity",
        F.col("last_reading_at").alias("detected_at")
    )
)

# COMMAND ----------

df_water_temp_alerts = (
    df_current
    .filter(
        F.col("water_temp_status").isin("LOW", "HIGH")
    )
    .select(
        "farm_id",
        "zone_id",
        "crop_type",
        "location",
        "state",
        F.concat(
            F.lit("WATER_TEMP_"),
            F.col("water_temp_status")
        ).alias("alert_type"),
        F.col("water_temp_c").alias("current_value"),
        F.col("water_temp_min_c").alias("min_threshold"),
        F.col("water_temp_max_c").alias("max_threshold"),
        F.col("water_temp_status").alias("metric_status"),
        "severity",
        F.col("last_reading_at").alias("detected_at")
    )
)

# COMMAND ----------

df_water_level_alerts = (
    df_current
    .filter(
        F.col("water_level_status").isin("LOW", "HIGH")
    )
    .select(
        "farm_id",
        "zone_id",
        "crop_type",
        "location",
        "state",
        F.concat(
            F.lit("WATER_LEVEL_"),
            F.col("water_level_status")
        ).alias("alert_type"),
        F.col("water_level_cm").alias("current_value"),
        F.col("water_level_min_cm").alias("min_threshold"),
        F.col("water_level_max_cm").alias("max_threshold"),
        F.col("water_level_status").alias("metric_status"),
        "severity",
        F.col("last_reading_at").alias("detected_at")
    )
)

# COMMAND ----------

df_humidity_alerts = (
    df_current
    .filter(
        F.col("humidity_status").isin("LOW", "HIGH")
    )
    .select(
        "farm_id",
        "zone_id",
        "crop_type",
        "location",
        "state",
        F.concat(
            F.lit("HUMIDITY_"),
            F.col("humidity_status")
        ).alias("alert_type"),
        F.col("humidity_pct").alias("current_value"),
        F.col("humidity_min_pct").alias("min_threshold"),
        F.col("humidity_max_pct").alias("max_threshold"),
        F.col("humidity_status").alias("metric_status"),
        "severity",
        F.col("last_reading_at").alias("detected_at")
    )
)

# COMMAND ----------

df_do_alerts = (
    df_current
    .filter(F.col("do_status") == "LOW")
    .select(
        "farm_id",
        "zone_id",
        "crop_type",
        "location",
        "state",
        F.lit("DISSOLVED_OXYGEN_LOW").alias("alert_type"),
        F.col("dissolved_oxygen_mg_l").alias("current_value"),
        F.col("do_min_mg_l").alias("min_threshold"),
        F.lit(None).cast("double").alias("max_threshold"),
        F.col("do_status").alias("metric_status"),
        "severity",
        F.col("last_reading_at").alias("detected_at")
    )
)

# COMMAND ----------

df_current_alerts = (
    df_ph_alerts
    .unionByName(df_ec_alerts)
    .unionByName(df_water_temp_alerts)
    .unionByName(df_water_level_alerts)
    .unionByName(df_humidity_alerts)
    .unionByName(df_do_alerts)
)

# COMMAND ----------

print("Current active alert conditions:", df_current_alerts.count())

# COMMAND ----------

display(
    df_current_alerts
    .orderBy(
        "farm_id",
        "zone_id",
        "alert_type"
    )
)

# COMMAND ----------

df_current_alerts = (
    df_current_alerts
    .withColumn(
        "alert_key",
        F.concat_ws(
            "|",
            "farm_id",
            "zone_id",
            "alert_type"
        )
    )
)

# COMMAND ----------

df_current_alerts = (
    df_current_alerts

    .withColumn(
        "alert_id",
        F.sha2(
            F.concat_ws(
                "|",
                "alert_key",
                F.col("detected_at").cast("string")
            ),
            256
        )
    )

    .withColumn(
        "first_detected_at",
        F.col("detected_at")
    )

    .withColumn(
        "last_detected_at",
        F.col("detected_at")
    )

    .withColumn(
        "alert_status",
        F.lit("ACTIVE")
    )

    .withColumn(
        "resolved_at",
        F.lit(None).cast("timestamp")
    )

    .withColumn(
        "gold_updated_at",
        F.current_timestamp()
    )

    .drop("detected_at")
)

# COMMAND ----------



# COMMAND ----------

if not spark.catalog.tableExists(GOLD_ALERTS_TABLE):

    (
        df_current_alerts
        .write
        .format("delta")
        .saveAsTable(GOLD_ALERTS_TABLE)
    )

    print(f"Created {GOLD_ALERTS_TABLE}")

# COMMAND ----------

if not spark.catalog.tableExists(GOLD_ALERTS_TABLE):

    (
        df_current_alerts
        .write
        .format("delta")
        .saveAsTable(GOLD_ALERTS_TABLE)
    )

    print(f"Created {GOLD_ALERTS_TABLE}")

else:

    gold_alert_target = DeltaTable.forName(
        spark,
        GOLD_ALERTS_TABLE
    )

    (
        gold_alert_target.alias("target")
        .merge(
            df_current_alerts.alias("source"),
            """
            target.alert_key = source.alert_key
            AND target.alert_status = 'ACTIVE'
            """
        )
        .whenMatchedUpdate(
            set={
                "current_value": "source.current_value",
                "min_threshold": "source.min_threshold",
                "max_threshold": "source.max_threshold",
                "metric_status": "source.metric_status",
                "severity": "source.severity",
                "last_detected_at": "source.last_detected_at",
                "gold_updated_at": "current_timestamp()"
            }
        )
        .whenNotMatchedInsertAll()
        .whenNotMatchedBySourceUpdate(
            condition="target.alert_status = 'ACTIVE'",
            set={
                "alert_status": "'RESOLVED'",
                "resolved_at": "current_timestamp()",
                "gold_updated_at": "current_timestamp()"
            }
        )
        .execute()
    )

    print(f"Updated {GOLD_ALERTS_TABLE}")

# COMMAND ----------

df_alerts_check = spark.table(GOLD_ALERTS_TABLE)

print(
    "Total alert records:",
    df_alerts_check.count()
)

# COMMAND ----------

display(
    df_alerts_check
    .groupBy(
        "alert_status"
    )
    .count()
)

# COMMAND ----------

display(
    df_alerts_check
    .filter(F.col("alert_status") == "ACTIVE")
    .select(
        "alert_id",
        "farm_id",
        "zone_id",
        "crop_type",
        "alert_type",
        "severity",
        "current_value",
        "min_threshold",
        "max_threshold",
        "first_detected_at",
        "last_detected_at"
    )
    .orderBy(
        F.col("severity").desc(),
        F.col("last_detected_at").desc()
    )
)

# COMMAND ----------

duplicate_active_alerts = (
    df_alerts_check
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