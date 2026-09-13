# Databricks notebook source
source_path = (
    "abfss://hydropulse@sthydropulsedev01.dfs.core.windows.net/"
    "bronze/farm_crop_master/farm_crop_master.csv"
)

print(source_path)

# COMMAND ----------

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    BooleanType
)

master_schema = StructType([
    StructField("farm_id", StringType(), False),
    StructField("zone_id", StringType(), False),
    StructField("location", StringType(), True),
    StructField("state", StringType(), True),
    StructField("timezone", StringType(), True),
    StructField("crop_type", StringType(), True),

    StructField("ph_min", DoubleType(), True),
    StructField("ph_max", DoubleType(), True),

    StructField("ec_min_ms_cm", DoubleType(), True),
    StructField("ec_max_ms_cm", DoubleType(), True),

    StructField("water_temp_min_c", DoubleType(), True),
    StructField("water_temp_max_c", DoubleType(), True),

    StructField("humidity_min_pct", DoubleType(), True),
    StructField("humidity_max_pct", DoubleType(), True),

    StructField("do_min_mg_l", DoubleType(), True),

    StructField("water_level_min_cm", DoubleType(), True),
    StructField("water_level_max_cm", DoubleType(), True),

    StructField("active_flag", BooleanType(), True)
])

# COMMAND ----------

source_path = (
    "abfss://hydropulse@sthydropulsedev01.dfs.core.windows.net/"
    "bronze/farm_crop_master/farm_crop_master.csv"
)

df_master = (
    spark.read
    .format("csv")
    .option("header", "true")
    .schema(master_schema)
    .load(source_path)
)

# COMMAND ----------

df_master.printSchema()
display(df_master)

print("Row count:", df_master.count())

# COMMAND ----------

delta_path = (
    "abfss://hydropulse@sthydropulsedev01.dfs.core.windows.net/"
    "bronze/delta/farm_crop_master/"
)

print(delta_path)

# COMMAND ----------

(
    df_master.write
    .format("delta")
    .mode("overwrite")
    .option("path", delta_path)
    .saveAsTable("hydropulse.bronze.bronze_farm_crop_master")
)

# COMMAND ----------

df_check = spark.table(
    "hydropulse.bronze.bronze_farm_crop_master"
)

display(df_check)

# COMMAND ----------

print("Delta table rows:", df_check.count())

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) AS total_records
# MAGIC FROM hydropulse.bronze.bronze_farm_crop_master;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED hydropulse.bronze.bronze_farm_crop_master;