# Databricks notebook source
eventhub_connection_string = dbutils.secrets.get(
    scope="hydropulse-secrets",
    key="eventhub-listen-connection"
)

print("Event Hub secret loaded successfully")

# COMMAND ----------

eventhub_namespace = "evhns-hydropulse-dev"
eventhub_name = "eh-hydropulse-telemetry"

bootstrap_servers = (
    f"{eventhub_namespace}.servicebus.windows.net:9093"
)

print("Bootstrap server:", bootstrap_servers)
print("Event Hub:", eventhub_name)

# COMMAND ----------

kafka_options = {
    "kafka.bootstrap.servers": bootstrap_servers,
    "subscribe": eventhub_name,

    "kafka.security.protocol": "SASL_SSL",
    "kafka.sasl.mechanism": "PLAIN",

    "kafka.sasl.jaas.config": (
        'kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule '
        'required '
        'username="$ConnectionString" '
        f'password="{eventhub_connection_string}";'
    ),

    "startingOffsets": "earliest",
    "failOnDataLoss": "false"
}

# COMMAND ----------

raw_stream = (
    spark.readStream
    .format("kafka")
    .options(**kafka_options)
    .load()
)

# COMMAND ----------

raw_stream.printSchema()

# COMMAND ----------

from pyspark.sql.functions import col

json_stream = raw_stream.select(
    col("value").cast("string").alias("json_value"),
    col("partition").alias("eventhub_partition"),
    col("offset").alias("eventhub_offset"),
    col("timestamp").alias("eventhub_enqueued_time")
)

# COMMAND ----------

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    LongType,
    DoubleType,
    BooleanType
)

# COMMAND ----------

telemetry_schema = StructType([
    StructField("event_id", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("schema_version", StringType(), True),

    StructField("farm_id", StringType(), True),
    StructField("zone_id", StringType(), True),
    StructField("device_id", StringType(), True),
    StructField("seq_num", LongType(), True),
    StructField("timestamp", StringType(), True),

    StructField("ph", DoubleType(), True),
    StructField("ec_ms_cm", DoubleType(), True),
    StructField("tds_ppm", DoubleType(), True),

    StructField("water_level_cm", DoubleType(), True),
    StructField("water_temp_c", DoubleType(), True),
    StructField("dissolved_oxygen_mg_l", DoubleType(), True),

    StructField("ambient_temp_c", DoubleType(), True),
    StructField("humidity_pct", DoubleType(), True),
    StructField("co2_ppm", DoubleType(), True),

    StructField("battery_voltage", DoubleType(), True),

    StructField("device_status", StringType(), True),
    StructField("controller_mode", StringType(), True),

    StructField("is_ph_reducer_on", BooleanType(), True),
    StructField("is_ph_increaser_on", BooleanType(), True),
    StructField("is_water_refill_on", BooleanType(), True),
    StructField("is_nutrient_pump_on", BooleanType(), True),
    StructField("is_humidifier_on", BooleanType(), True),
    StructField("is_exhaust_fan_on", BooleanType(), True),
    StructField("is_aerator_on", BooleanType(), True)
])

# COMMAND ----------

from pyspark.sql.functions import col, from_json, current_timestamp

# COMMAND ----------

parsed_stream = (
    json_stream
    .withColumn(
        "parsed",
        from_json(col("json_value"), telemetry_schema)
    )
)

# COMMAND ----------

bronze_stream = (
    parsed_stream
    .select(
        col("parsed.*"),

        col("eventhub_partition"),
        col("eventhub_offset"),
        col("eventhub_enqueued_time"),

        col("json_value").alias("raw_payload")
    )
    .withColumn(
        "bronze_ingestion_time",
        current_timestamp()
    )
)

# COMMAND ----------

bronze_stream.printSchema()

# COMMAND ----------

bronze_delta_path = (
    "abfss://hydropulse@sthydropulsedev01.dfs.core.windows.net/"
    "bronze/delta/iot_telemetry/"
)

checkpoint_path = (
    "abfss://hydropulse@sthydropulsedev01.dfs.core.windows.net/"
    "checkpoint/bronze_iot/"
)

# COMMAND ----------

bronze_query = (
    bronze_stream.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", checkpoint_path)
    .trigger(availableNow=True)
    .start(bronze_delta_path)
)

# COMMAND ----------

bronze_query.awaitTermination()

# COMMAND ----------

print("Bronze AvailableNow ingestion completed")

# COMMAND ----------

df_bronze_check = spark.read.format("delta").load(
    bronze_delta_path
)

print("Bronze records:", df_bronze_check.count())

# COMMAND ----------

display(df_bronze_check.limit(20))

# COMMAND ----------

df_bronze_check.groupBy(
    "eventhub_partition"
).count().orderBy(
    "eventhub_partition"
).show()

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS hydropulse.bronze.bronze_iot_telemetry
# MAGIC USING DELTA
# MAGIC LOCATION 'abfss://hydropulse@sthydropulsedev01.dfs.core.windows.net/bronze/delta/iot_telemetry/';

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) AS total_bronze_events
# MAGIC FROM hydropulse.bronze.bronze_iot_telemetry;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED
# MAGIC hydropulse.bronze.bronze_iot_telemetry;

# COMMAND ----------

count_before = spark.table(
    "hydropulse.bronze.bronze_iot_telemetry"
).count()

print("Before:", count_before)

# COMMAND ----------

bronze_query = (
    bronze_stream.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", checkpoint_path)
    .trigger(availableNow=True)
    .start(bronze_delta_path)
)

bronze_query.awaitTermination()

# COMMAND ----------

count_after = spark.table(
    "hydropulse.bronze.bronze_iot_telemetry"
).count()

print("Before:", count_before)
print("After :", count_after)
print("New events:", count_after - count_before)