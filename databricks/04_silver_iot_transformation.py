# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# COMMAND ----------

BRONZE_IOT_TABLE = "hydropulse.bronze.bronze_iot_telemetry"
BRONZE_MASTER_TABLE = "hydropulse.bronze.bronze_farm_crop_master"

SILVER_IOT_TABLE = "hydropulse.silver.silver_iot_enriched"
SILVER_INVALID_TABLE = "hydropulse.silver.silver_invalid_records"

# COMMAND ----------

df_iot_bronze = spark.table(BRONZE_IOT_TABLE)

df_master = spark.table(BRONZE_MASTER_TABLE)

# COMMAND ----------

print("Bronze IoT records:", df_iot_bronze.count())
print("Master records:", df_master.count())

# COMMAND ----------

spark.table(BRONZE_IOT_TABLE)

# COMMAND ----------

required_columns = [
    "event_id",
    "farm_id",
    "zone_id",
    "device_id",
    "seq_num",
    "timestamp",
    "ph",
    "ec_ms_cm",
    "water_level_cm",
    "water_temp_c",
    "dissolved_oxygen_mg_l"
]

# COMMAND ----------

required_fields_valid = F.lit(True)

for column_name in required_columns:
    required_fields_valid = (
        required_fields_valid
        & F.col(column_name).isNotNull()
    )

# COMMAND ----------

df_validated = (
    df_iot_bronze
    .withColumn(
        "required_fields_valid",
        required_fields_valid
    )
)

# COMMAND ----------

df_validated = (
    df_validated

    .withColumn(
        "ph_valid",
        F.col("ph").between(0.0, 14.0)
    )

    .withColumn(
        "ec_valid",
        F.col("ec_ms_cm").between(0.0, 10.0)
    )

    .withColumn(
        "water_temp_valid",
        F.col("water_temp_c").between(0.0, 50.0)
    )

    .withColumn(
        "water_level_valid",
        F.col("water_level_cm").between(0.0, 100.0)
    )

    .withColumn(
        "do_valid",
        F.col("dissolved_oxygen_mg_l").between(0.0, 20.0)
    )

    .withColumn(
        "humidity_valid",
        F.col("humidity_pct").between(0.0, 100.0)
    )
)

# COMMAND ----------

df_validated = (
    df_validated
    .withColumn(
        "is_valid_record",
        F.col("required_fields_valid")
        & F.col("ph_valid")
        & F.col("ec_valid")
        & F.col("water_temp_valid")
        & F.col("water_level_valid")
        & F.col("do_valid")
        & F.col("humidity_valid")
    )
)

# COMMAND ----------

df_validated = (
    df_validated
    .withColumn(
        "validation_error",
        F.when(
            ~F.col("required_fields_valid"),
            F.lit("MISSING_REQUIRED_FIELD")
        )
        .when(
            ~F.col("ph_valid"),
            F.lit("INVALID_PH")
        )
        .when(
            ~F.col("ec_valid"),
            F.lit("INVALID_EC")
        )
        .when(
            ~F.col("water_temp_valid"),
            F.lit("INVALID_WATER_TEMPERATURE")
        )
        .when(
            ~F.col("water_level_valid"),
            F.lit("INVALID_WATER_LEVEL")
        )
        .when(
            ~F.col("do_valid"),
            F.lit("INVALID_DISSOLVED_OXYGEN")
        )
        .when(
            ~F.col("humidity_valid"),
            F.lit("INVALID_HUMIDITY")
        )
    )
)

# COMMAND ----------

df_valid = (
    df_validated
    .filter(F.col("is_valid_record"))
)

df_invalid = (
    df_validated
    .filter(~F.col("is_valid_record"))
)

# COMMAND ----------

df_invalid = (
    df_invalid
    .withColumn(
        "rejected_at",
        F.current_timestamp()
    )
    .withColumn(
        "rejection_layer",
        F.lit("SILVER")
    )
)

# COMMAND ----------

print("Total Bronze :", df_iot_bronze.count())
print("Valid        :", df_valid.count())
print("Invalid      :", df_invalid.count())

# COMMAND ----------

display(
    df_invalid.select(
        "event_id",
        "farm_id",
        "zone_id",
        "device_id",
        "validation_error",
        "raw_payload"
    )
)

# COMMAND ----------

df_duplicate_check = (
    df_valid
    .groupBy("event_id")
    .count()
    .filter(F.col("count") > 1)
)

print("Duplicate event IDs:", df_duplicate_check.count())

# COMMAND ----------

display(
    df_duplicate_check
    .orderBy(F.col("count").desc())
)

# COMMAND ----------

dedup_window = (
    Window
    .partitionBy("event_id")
    .orderBy(
        F.col("eventhub_enqueued_time").asc(),
        F.col("eventhub_partition").asc(),
        F.col("eventhub_offset").asc()
    )
)

# COMMAND ----------

df_ranked = (
    df_valid
    .withColumn(
        "dedup_rank",
        F.row_number().over(dedup_window)
    )
)

# COMMAND ----------

df_deduplicated = (
    df_ranked
    .filter(F.col("dedup_rank") == 1)
    .drop("dedup_rank")
)

# COMMAND ----------

valid_count = df_valid.count()
deduplicated_count = df_deduplicated.count()

print("Valid records       :", valid_count)
print("After deduplication :", deduplicated_count)
print("Duplicates removed  :", valid_count - deduplicated_count)

# COMMAND ----------

remaining_duplicates = (
    df_deduplicated
    .groupBy("event_id")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

print("Remaining duplicate event IDs:", remaining_duplicates)

# COMMAND ----------

df_sequence = (
    df_deduplicated
    .withColumn(
        "event_timestamp",
        F.to_timestamp("timestamp")
    )
)

# COMMAND ----------

sequence_window = (
    Window
    .partitionBy(
        "farm_id",
        "zone_id",
        "device_id"
    )
    .orderBy(
        F.col("seq_num")
    )
)

# COMMAND ----------

df_sequence = (
    df_sequence
    .withColumn(
        "previous_seq_num",
        F.lag("seq_num").over(sequence_window)
    )
)

# COMMAND ----------

df_sequence = (
    df_sequence
    .withColumn(
        "missing_packet_count",
        F.when(
            F.col("previous_seq_num").isNull(),
            F.lit(0)
        )
        .when(
            F.col("seq_num") > F.col("previous_seq_num") + 1,
            F.col("seq_num") - F.col("previous_seq_num") - 1
        )
        .otherwise(F.lit(0))
    )
)

# COMMAND ----------

df_sequence = (
    df_sequence
    .withColumn(
        "has_sequence_gap",
        F.col("missing_packet_count") > 0
    )
)

# COMMAND ----------

packet_gap_count = (
    df_sequence
    .filter(F.col("has_sequence_gap"))
    .count()
)

total_missing_packets = (
    df_sequence
    .agg(
        F.sum("missing_packet_count")
        .alias("total_missing_packets")
    )
    .first()["total_missing_packets"]
)

print("Sequence gaps detected :", packet_gap_count)
print("Estimated lost packets :", total_missing_packets)

# COMMAND ----------

display(
    df_sequence
    .filter(F.col("has_sequence_gap"))
    .select(
        "farm_id",
        "zone_id",
        "device_id",
        "previous_seq_num",
        "seq_num",
        "missing_packet_count",
        "event_timestamp"
    )
    .orderBy(
        "farm_id",
        "zone_id",
        "device_id",
        "seq_num"
    )
)

# COMMAND ----------

master_columns = [
    "farm_id",
    "zone_id",
    "location",
    "state",
    "timezone",
    "crop_type",
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
    "active_flag"
]

df_master_selected = df_master.select(*master_columns)

# COMMAND ----------

df_master_duplicates = (
    df_master_selected
    .groupBy("farm_id", "zone_id")
    .count()
    .filter(F.col("count") > 1)
)

master_duplicate_count = df_master_duplicates.count()

print("Duplicate master keys:", master_duplicate_count)

# COMMAND ----------

df_enriched = (
    df_sequence.alias("iot")
    .join(
        df_master_selected.alias("master"),
        on=["farm_id", "zone_id"],
        how="left"
    )
)

# COMMAND ----------

df_enriched = (
    df_enriched
    .withColumn(
        "master_data_matched",
        F.col("crop_type").isNotNull()
    )
)

# COMMAND ----------

before_join_count = df_sequence.count()

# COMMAND ----------

after_join_count = df_enriched.count()

print("Before join :", before_join_count)
print("After join  :", after_join_count)

# COMMAND ----------

if before_join_count != after_join_count:
    raise ValueError(
        "Row count changed after master join. "
        "Check duplicate farm_id + zone_id keys in master data."
    )

# COMMAND ----------

unmatched_count = (
    df_enriched
    .filter(~F.col("master_data_matched"))
    .count()
)

print("Unmatched master records:", unmatched_count)

# COMMAND ----------

display(
    df_enriched
    .filter(~F.col("master_data_matched"))
    .select(
        "event_id",
        "farm_id",
        "zone_id",
        "device_id",
        "event_timestamp"
    )
)

# COMMAND ----------

display(
    df_enriched.select(
        "event_id",
        "farm_id",
        "zone_id",
        "device_id",
        "event_timestamp",

        "location",
        "crop_type",

        "ph",
        "ph_min",
        "ph_max",

        "ec_ms_cm",
        "ec_min_ms_cm",
        "ec_max_ms_cm",

        "master_data_matched"
    ).limit(20)
)

# COMMAND ----------

display(
    df_enriched
    .groupBy("crop_type")
    .count()
    .orderBy(F.col("count").desc())
)

# COMMAND ----------

def range_status(value_col, min_col, max_col):
    return (
        F.when(
            F.col(min_col).isNull() | F.col(max_col).isNull(),
            F.lit("UNKNOWN")
        )
        .when(
            F.col(value_col) < F.col(min_col),
            F.lit("LOW")
        )
        .when(
            F.col(value_col) > F.col(max_col),
            F.lit("HIGH")
        )
        .otherwise(
            F.lit("NORMAL")
        )
    )

# COMMAND ----------

def minimum_status(value_col, min_col):
    return (
        F.when(
            F.col(min_col).isNull(),
            F.lit("UNKNOWN")
        )
        .when(
            F.col(value_col) < F.col(min_col),
            F.lit("LOW")
        )
        .otherwise(
            F.lit("NORMAL")
        )
    )

# COMMAND ----------

df_status = (
    df_enriched

    .withColumn(
        "ph_status",
        range_status("ph", "ph_min", "ph_max")
    )

    .withColumn(
        "ec_status",
        range_status(
            "ec_ms_cm",
            "ec_min_ms_cm",
            "ec_max_ms_cm"
        )
    )

    .withColumn(
        "water_temp_status",
        range_status(
            "water_temp_c",
            "water_temp_min_c",
            "water_temp_max_c"
        )
    )

    .withColumn(
        "water_level_status",
        range_status(
            "water_level_cm",
            "water_level_min_cm",
            "water_level_max_cm"
        )
    )

    .withColumn(
        "humidity_status",
        range_status(
            "humidity_pct",
            "humidity_min_pct",
            "humidity_max_pct"
        )
    )

    .withColumn(
        "do_status",
        minimum_status(
            "dissolved_oxygen_mg_l",
            "do_min_mg_l"
        )
    )
)

# COMMAND ----------

status_columns = [
    "ph_status",
    "ec_status",
    "water_temp_status",
    "water_level_status",
    "humidity_status",
    "do_status"
]

# COMMAND ----------

anomaly_count_expression = F.lit(0)

for status_col in status_columns:
    anomaly_count_expression = (
        anomaly_count_expression
        + F.when(
            F.col(status_col).isin("LOW", "HIGH"),
            1
        ).otherwise(0)
    )

# COMMAND ----------

df_status = (
    df_status
    .withColumn(
        "anomaly_count",
        anomaly_count_expression
    )
)

# COMMAND ----------

df_status = (
    df_status
    .withColumn(
        "is_anomaly",
        F.col("anomaly_count") > 0
    )
)

# COMMAND ----------

df_status = (
    df_status
    .withColumn(
        "severity",
        F.when(
            ~F.col("master_data_matched"),
            F.lit("UNKNOWN")
        )
        .when(
            F.col("anomaly_count") >= 3,
            F.lit("HIGH")
        )
        .when(
            F.col("anomaly_count") == 2,
            F.lit("MEDIUM")
        )
        .when(
            F.col("anomaly_count") == 1,
            F.lit("LOW")
        )
        .otherwise(
            F.lit("NONE")
        )
    )
)

# COMMAND ----------

df_status = (
    df_status
    .withColumn(
        "overall_status",
        F.when(
            ~F.col("master_data_matched"),
            F.lit("UNKNOWN")
        )
        .when(
            F.col("anomaly_count") >= 3,
            F.lit("CRITICAL")
        )
        .when(
            F.col("anomaly_count") >= 1,
            F.lit("ATTENTION")
        )
        .otherwise(
            F.lit("HEALTHY")
        )
    )
)

# COMMAND ----------

df_status = (
    df_status
    .withColumn(
        "silver_processed_at",
        F.current_timestamp()
    )
)

# COMMAND ----------

display(
    df_status
    .groupBy("overall_status")
    .count()
    .orderBy(F.col("count").desc())
)

# COMMAND ----------

display(
    df_status
    .groupBy("severity")
    .count()
    .orderBy(F.col("count").desc())
)

# COMMAND ----------

for status_col in status_columns:
    print(f"\n{status_col}")

    (
        df_status
        .groupBy(status_col)
        .count()
        .orderBy(F.col("count").desc())
        .show()
    )

# COMMAND ----------

display(
    df_status
    .filter(F.col("is_anomaly"))
    .select(
        "event_id",
        "farm_id",
        "zone_id",
        "device_id",
        "crop_type",
        "event_timestamp",

        "ph",
        "ph_status",

        "ec_ms_cm",
        "ec_status",

        "water_temp_c",
        "water_temp_status",

        "water_level_cm",
        "water_level_status",

        "humidity_pct",
        "humidity_status",

        "dissolved_oxygen_mg_l",
        "do_status",

        "anomaly_count",
        "severity",
        "overall_status"
    )
    .orderBy(
        F.col("event_timestamp").desc()
    )
)

# COMMAND ----------

display(
    df_status
    .filter(
        F.col("ph_status") != "NORMAL"
    )
    .select(
        "crop_type",
        "ph",
        "ph_min",
        "ph_max",
        "ph_status"
    )
)

# COMMAND ----------

df_enrichment_invalid = (
    df_status
    .filter(~F.col("master_data_matched"))
    .withColumn(
        "validation_error",
        F.lit("MASTER_DATA_NOT_FOUND")
    )
    .withColumn(
        "rejected_at",
        F.current_timestamp()
    )
    .withColumn(
        "rejection_layer",
        F.lit("SILVER")
    )
)

# COMMAND ----------

df_silver = (
    df_status
    .filter(F.col("master_data_matched"))
)

# COMMAND ----------

silver_columns = [
    # Event identity
    "event_id",
    "event_type",
    "schema_version",

    # Device identity
    "farm_id",
    "zone_id",
    "device_id",
    "seq_num",

    # Time
    "event_timestamp",

    # Master/reference attributes
    "location",
    "state",
    "timezone",
    "crop_type",

    # Sensor measurements
    "ph",
    "ec_ms_cm",
    "tds_ppm",
    "water_level_cm",
    "water_temp_c",
    "dissolved_oxygen_mg_l",
    "ambient_temp_c",
    "humidity_pct",
    "co2_ppm",
    "battery_voltage",

    # Device/controller information
    "device_status",
    "controller_mode",

    # Actuator states
    "is_ph_reducer_on",
    "is_ph_increaser_on",
    "is_water_refill_on",
    "is_nutrient_pump_on",
    "is_humidifier_on",
    "is_exhaust_fan_on",
    "is_aerator_on",

    # Crop thresholds
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

    # Data-quality / network information
    "previous_seq_num",
    "missing_packet_count",
    "has_sequence_gap",

    # Derived sensor statuses
    "ph_status",
    "ec_status",
    "water_temp_status",
    "water_level_status",
    "humidity_status",
    "do_status",

    # Business status
    "is_anomaly",
    "anomaly_count",
    "severity",
    "overall_status",

    # Pipeline metadata
    "eventhub_partition",
    "eventhub_offset",
    "eventhub_enqueued_time",
    "bronze_ingestion_time",
    "silver_processed_at"
]

# COMMAND ----------

df_silver_final = df_silver.select(*silver_columns)

# COMMAND ----------

source_duplicate_count = (
    df_silver_final
    .groupBy("event_id")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

print("Duplicate event IDs before Silver write:", source_duplicate_count)

# COMMAND ----------

from delta.tables import DeltaTable

# COMMAND ----------

if not spark.catalog.tableExists(SILVER_IOT_TABLE):

    (
        df_silver_final
        .write
        .format("delta")
        .mode("overwrite")
        .saveAsTable(SILVER_IOT_TABLE)
    )

    print("Created:", SILVER_IOT_TABLE)

else:
    print("Table already exists:", SILVER_IOT_TABLE)

# COMMAND ----------

from delta.tables import DeltaTable

if not spark.catalog.tableExists(SILVER_IOT_TABLE):

    (
        df_silver_final
        .write
        .format("delta")
        .saveAsTable(SILVER_IOT_TABLE)
    )

    print(f"Created {SILVER_IOT_TABLE}")

else:

    silver_target = DeltaTable.forName(
        spark,
        SILVER_IOT_TABLE
    )

    (
        silver_target.alias("target")
        .merge(
            df_silver_final.alias("source"),
            "target.event_id = source.event_id"
        )
        .whenNotMatchedInsertAll()
        .execute()
    )

    print(f"Merged data into {SILVER_IOT_TABLE}")

# COMMAND ----------

invalid_columns = [
    "event_id",
    "farm_id",
    "zone_id",
    "device_id",
    "seq_num",
    "timestamp",

    "ph",
    "ec_ms_cm",
    "water_level_cm",
    "water_temp_c",
    "dissolved_oxygen_mg_l",
    "humidity_pct",

    "eventhub_partition",
    "eventhub_offset",
    "eventhub_enqueued_time",
    "bronze_ingestion_time",

    "raw_payload",
    "validation_error",
    "rejected_at",
    "rejection_layer"
]

# COMMAND ----------

df_invalid_final = (
    df_invalid
    .select(*invalid_columns)
)

# COMMAND ----------

df_enrichment_invalid_final = (
    df_enrichment_invalid
    .select(*invalid_columns)
)

# COMMAND ----------

df_all_invalid = (
    df_invalid_final
    .unionByName(
        df_enrichment_invalid_final,
        allowMissingColumns=True
    )
)

# COMMAND ----------

if not spark.catalog.tableExists(SILVER_INVALID_TABLE):

    (
        df_all_invalid
        .write
        .format("delta")
        .saveAsTable(SILVER_INVALID_TABLE)
    )

    print(f"Created {SILVER_INVALID_TABLE}")

else:

    invalid_target = DeltaTable.forName(
        spark,
        SILVER_INVALID_TABLE
    )

    (
        invalid_target.alias("target")
        .merge(
            df_all_invalid.alias("source"),
            """
            target.eventhub_partition = source.eventhub_partition
            AND target.eventhub_offset = source.eventhub_offset
            """
        )
        .whenNotMatchedInsertAll()
        .execute()
    )

    print(f"Merged data into {SILVER_INVALID_TABLE}")

# COMMAND ----------

silver_count = spark.table(SILVER_IOT_TABLE).count()
invalid_count = spark.table(SILVER_INVALID_TABLE).count()

print("Silver valid records  :", silver_count)
print("Silver invalid records:", invalid_count)

# COMMAND ----------

silver_duplicates = (
    spark.table(SILVER_IOT_TABLE)
    .groupBy("event_id")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

print("Silver duplicate event IDs:", silver_duplicates)

# COMMAND ----------

spark.table(SILVER_IOT_TABLE).printSchema()

# COMMAND ----------

display(
    spark.table(SILVER_INVALID_TABLE)
    .groupBy("validation_error")
    .count()
)

# COMMAND ----------

display(
    spark.table(SILVER_IOT_TABLE)
    .select(
        "farm_id",
        "zone_id",
        "device_id",
        "crop_type",
        "event_timestamp",
        "ph",
        "ph_status",
        "ec_ms_cm",
        "ec_status",
        "humidity_pct",
        "humidity_status",
        "missing_packet_count",
        "is_anomaly",
        "severity",
        "overall_status"
    )
    .orderBy(
        F.col("event_timestamp").desc()
    )
    .limit(50)
)

# COMMAND ----------

df_enrichment_invalid = (
    df_status
    .filter(~F.col("master_data_matched"))
    .withColumn(
        "validation_error",
        F.lit("MASTER_DATA_NOT_FOUND")
    )
    .withColumn(
        "rejected_at",
        F.current_timestamp()
    )
    .withColumn(
        "rejection_layer",
        F.lit("SILVER")
    )
)

df_silver = (
    df_status
    .filter(F.col("master_data_matched"))
)

# COMMAND ----------

silver_columns = [
    # Event identity
    "event_id",
    "event_type",
    "schema_version",

    # Device identity
    "farm_id",
    "zone_id",
    "device_id",
    "seq_num",

    # Time
    "event_timestamp",

    # Farm / crop information
    "location",
    "state",
    "timezone",
    "crop_type",

    # Sensor measurements
    "ph",
    "ec_ms_cm",
    "tds_ppm",
    "water_level_cm",
    "water_temp_c",
    "dissolved_oxygen_mg_l",
    "ambient_temp_c",
    "humidity_pct",
    "co2_ppm",
    "battery_voltage",

    # Device / controller
    "device_status",
    "controller_mode",

    # Actuators
    "is_ph_reducer_on",
    "is_ph_increaser_on",
    "is_water_refill_on",
    "is_nutrient_pump_on",
    "is_humidifier_on",
    "is_exhaust_fan_on",
    "is_aerator_on",

    # Crop thresholds
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

    # Network quality
    "previous_seq_num",
    "missing_packet_count",
    "has_sequence_gap",

    # Sensor status
    "ph_status",
    "ec_status",
    "water_temp_status",
    "water_level_status",
    "humidity_status",
    "do_status",

    # Business status
    "is_anomaly",
    "anomaly_count",
    "severity",
    "overall_status",

    # Pipeline metadata
    "eventhub_partition",
    "eventhub_offset",
    "eventhub_enqueued_time",
    "bronze_ingestion_time",
    "silver_processed_at"
]

df_silver_final = df_silver.select(*silver_columns)

# COMMAND ----------

source_duplicate_count = (
    df_silver_final
    .groupBy("event_id")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

print(
    "Duplicate event IDs before Silver write:",
    source_duplicate_count
)

# COMMAND ----------

from delta.tables import DeltaTable

# COMMAND ----------

if not spark.catalog.tableExists(SILVER_IOT_TABLE):

    (
        df_silver_final
        .write
        .format("delta")
        .saveAsTable(SILVER_IOT_TABLE)
    )

    print(f"Created {SILVER_IOT_TABLE}")

else:

    silver_target = DeltaTable.forName(
        spark,
        SILVER_IOT_TABLE
    )

    (
        silver_target.alias("target")
        .merge(
            df_silver_final.alias("source"),
            "target.event_id = source.event_id"
        )
        .whenNotMatchedInsertAll()
        .execute()
    )

    print(f"Merged data into {SILVER_IOT_TABLE}")

# COMMAND ----------

silver_count = spark.table(SILVER_IOT_TABLE).count()

print("Silver records:", silver_count)

# COMMAND ----------

display(
    spark.table(SILVER_IOT_TABLE)
    .select(
        "event_id",
        "farm_id",
        "zone_id",
        "device_id",
        "crop_type",
        "event_timestamp",
        "ph",
        "ph_status",
        "ec_ms_cm",
        "ec_status",
        "humidity_pct",
        "humidity_status",
        "missing_packet_count",
        "is_anomaly",
        "severity",
        "overall_status"
    )
    .orderBy(F.col("event_timestamp").desc())
    .limit(20)
)

# COMMAND ----------

invalid_columns = [
    "event_id",
    "farm_id",
    "zone_id",
    "device_id",
    "seq_num",
    "timestamp",

    "ph",
    "ec_ms_cm",
    "water_level_cm",
    "water_temp_c",
    "dissolved_oxygen_mg_l",
    "humidity_pct",

    "eventhub_partition",
    "eventhub_offset",
    "eventhub_enqueued_time",
    "bronze_ingestion_time",

    "raw_payload",
    "validation_error",
    "rejected_at",
    "rejection_layer"
]

# COMMAND ----------

df_invalid_final = (
    df_invalid
    .select(*invalid_columns)
)

# COMMAND ----------

df_enrichment_invalid_final = (
    df_enrichment_invalid
    .select(*invalid_columns)
)

# COMMAND ----------

df_all_invalid = (
    df_invalid_final
    .unionByName(
        df_enrichment_invalid_final,
        allowMissingColumns=True
    )
)

# COMMAND ----------

if not spark.catalog.tableExists(SILVER_INVALID_TABLE):

    (
        df_all_invalid
        .write
        .format("delta")
        .saveAsTable(SILVER_INVALID_TABLE)
    )

    print(f"Created {SILVER_INVALID_TABLE}")

else:

    invalid_target = DeltaTable.forName(
        spark,
        SILVER_INVALID_TABLE
    )

    (
        invalid_target.alias("target")
        .merge(
            df_all_invalid.alias("source"),
            """
            target.eventhub_partition = source.eventhub_partition
            AND target.eventhub_offset = source.eventhub_offset
            """
        )
        .whenNotMatchedInsertAll()
        .execute()
    )

    print(f"Merged data into {SILVER_INVALID_TABLE}")

# COMMAND ----------

silver_count = spark.table(SILVER_IOT_TABLE).count()
invalid_count = spark.table(SILVER_INVALID_TABLE).count()

silver_duplicates = (
    spark.table(SILVER_IOT_TABLE)
    .groupBy("event_id")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

print("Silver valid records   :", silver_count)
print("Silver invalid records :", invalid_count)
print("Duplicate event IDs    :", silver_duplicates)

# COMMAND ----------

display(
    spark.table(SILVER_INVALID_TABLE)
    .groupBy("validation_error")
    .count()
)