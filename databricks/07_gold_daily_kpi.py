# Databricks notebook source
from pyspark.sql import functions as F
from delta.tables import DeltaTable

SILVER_IOT_TABLE = "hydropulse.silver.silver_iot_enriched"
GOLD_DAILY_KPI_TABLE = "hydropulse.gold.gold_daily_kpi"

# COMMAND ----------

df_silver = spark.table(SILVER_IOT_TABLE)

print("Silver records:", df_silver.count())

# COMMAND ----------

df_daily_source = (
    df_silver
    .withColumn(
        "local_event_timestamp",
        F.expr(
            "from_utc_timestamp(event_timestamp, timezone)"
        )
    )
    .withColumn(
        "kpi_date",
        F.to_date("local_event_timestamp")
    )
)

# COMMAND ----------

df_daily_kpi = (
    df_daily_source

    .groupBy(
        "kpi_date",
        "farm_id",
        "zone_id",
        "crop_type",
        "location",
        "state",
        "timezone"
    )

    .agg(
        # Volume / quality
        F.count("*").alias("reading_count"),

        F.sum(
            F.col("is_anomaly").cast("int")
        ).alias("anomaly_reading_count"),

        F.sum(
            F.col("missing_packet_count")
        ).alias("missing_packet_count"),

        # Sensor averages
        F.avg("ph").alias("avg_ph"),
        F.avg("ec_ms_cm").alias("avg_ec_ms_cm"),
        F.avg("water_temp_c").alias("avg_water_temp_c"),
        F.avg("water_level_cm").alias("avg_water_level_cm"),
        F.avg("dissolved_oxygen_mg_l").alias("avg_do_mg_l"),
        F.avg("ambient_temp_c").alias("avg_ambient_temp_c"),
        F.avg("humidity_pct").alias("avg_humidity_pct"),
        F.avg("co2_ppm").alias("avg_co2_ppm"),

        # Actuator runtime %
        (
            F.avg(F.col("is_ph_reducer_on").cast("int")) * 100
        ).alias("ph_reducer_runtime_pct"),

        (
            F.avg(F.col("is_ph_increaser_on").cast("int")) * 100
        ).alias("ph_increaser_runtime_pct"),

        (
            F.avg(F.col("is_water_refill_on").cast("int")) * 100
        ).alias("water_refill_runtime_pct"),

        (
            F.avg(F.col("is_nutrient_pump_on").cast("int")) * 100
        ).alias("nutrient_pump_runtime_pct"),

        (
            F.avg(F.col("is_humidifier_on").cast("int")) * 100
        ).alias("humidifier_runtime_pct"),

        (
            F.avg(F.col("is_exhaust_fan_on").cast("int")) * 100
        ).alias("exhaust_fan_runtime_pct"),

        (
            F.avg(F.col("is_aerator_on").cast("int")) * 100
        ).alias("aerator_runtime_pct"),

        # Time coverage
        F.min("local_event_timestamp").alias("first_reading_at"),
        F.max("local_event_timestamp").alias("last_reading_at")
    )
)

# COMMAND ----------

df_daily_kpi = (
    df_daily_kpi
    .withColumn(
        "anomaly_rate_pct",
        F.round(
            (
                F.col("anomaly_reading_count")
                / F.col("reading_count")
            ) * 100,
            2
        )
    )
)

# COMMAND ----------

df_daily_kpi = (
    df_daily_kpi
    .withColumn(
        "health_score",
        F.round(
            F.greatest(
                F.lit(0.0),
                F.lit(100.0) - F.col("anomaly_rate_pct")
            ),
            2
        )
    )
)

# COMMAND ----------

df_daily_kpi = (
    df_daily_kpi
    .withColumn(
        "packet_gap_rate_pct",
        F.round(
            (
                F.col("missing_packet_count")
                / (
                    F.col("reading_count")
                    + F.col("missing_packet_count")
                )
            ) * 100,
            2
        )
    )
)

# COMMAND ----------

df_daily_kpi = (
    df_daily_kpi
    .withColumn(
        "gold_updated_at",
        F.current_timestamp()
    )
)

# COMMAND ----------

df_daily_kpi = (
    df_daily_kpi
    .withColumn("avg_ph", F.round("avg_ph", 2))
    .withColumn("avg_ec_ms_cm", F.round("avg_ec_ms_cm", 2))
    .withColumn("avg_water_temp_c", F.round("avg_water_temp_c", 2))
    .withColumn("avg_water_level_cm", F.round("avg_water_level_cm", 2))
    .withColumn("avg_do_mg_l", F.round("avg_do_mg_l", 2))
    .withColumn("avg_ambient_temp_c", F.round("avg_ambient_temp_c", 2))
    .withColumn("avg_humidity_pct", F.round("avg_humidity_pct", 2))
    .withColumn("avg_co2_ppm", F.round("avg_co2_ppm", 2))

    .withColumn(
        "ph_reducer_runtime_pct",
        F.round("ph_reducer_runtime_pct", 2)
    )
    .withColumn(
        "ph_increaser_runtime_pct",
        F.round("ph_increaser_runtime_pct", 2)
    )
    .withColumn(
        "water_refill_runtime_pct",
        F.round("water_refill_runtime_pct", 2)
    )
    .withColumn(
        "nutrient_pump_runtime_pct",
        F.round("nutrient_pump_runtime_pct", 2)
    )
    .withColumn(
        "humidifier_runtime_pct",
        F.round("humidifier_runtime_pct", 2)
    )
    .withColumn(
        "exhaust_fan_runtime_pct",
        F.round("exhaust_fan_runtime_pct", 2)
    )
    .withColumn(
        "aerator_runtime_pct",
        F.round("aerator_runtime_pct", 2)
    )
)

# COMMAND ----------

duplicate_kpi_keys = (
    df_daily_kpi
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
    duplicate_kpi_keys
)

# COMMAND ----------

display(
    df_daily_kpi
    .select(
        "kpi_date",
        "farm_id",
        "zone_id",
        "crop_type",
        "location",
        "reading_count",
        "anomaly_reading_count",
        "anomaly_rate_pct",
        "missing_packet_count",
        "packet_gap_rate_pct",
        "avg_ph",
        "avg_ec_ms_cm",
        "avg_water_temp_c",
        "avg_humidity_pct",
        "health_score"
    )
    .orderBy(
        F.col("kpi_date").desc(),
        "farm_id",
        "zone_id"
    )
)

# COMMAND ----------

if not spark.catalog.tableExists(GOLD_DAILY_KPI_TABLE):

    (
        df_daily_kpi
        .write
        .format("delta")
        .saveAsTable(GOLD_DAILY_KPI_TABLE)
    )

    print(f"Created {GOLD_DAILY_KPI_TABLE}")

else:

    gold_daily_target = DeltaTable.forName(
        spark,
        GOLD_DAILY_KPI_TABLE
    )

    (
        gold_daily_target.alias("target")
        .merge(
            df_daily_kpi.alias("source"),
            """
            target.kpi_date = source.kpi_date
            AND target.farm_id = source.farm_id
            AND target.zone_id = source.zone_id
            """
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )

    print(f"Updated {GOLD_DAILY_KPI_TABLE}")

# COMMAND ----------

df_kpi_check = spark.table(GOLD_DAILY_KPI_TABLE)

print(
    "Gold daily KPI rows:",
    df_kpi_check.count()
)

# COMMAND ----------

duplicate_count = (
    df_kpi_check
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
    "Duplicate KPI keys:",
    duplicate_count
)

# COMMAND ----------

df_kpi_check.select(
    F.min("health_score").alias("min_health_score"),
    F.max("health_score").alias("max_health_score")
).show()