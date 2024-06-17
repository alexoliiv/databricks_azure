# Databricks notebook source
# MAGIC %md 
# MAGIC
# MAGIC #### Libraries and configuration

# COMMAND ----------

from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    StringType,
    FloatType,
    DateType
)
from pyspark.sql.functions import col, current_timestamp, lit, to_timestamp, concat

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Step 1 - Reading parquet files

# COMMAND ----------

races_df = spark.read.parquet(f"{silver_folder_path}/races/")

# COMMAND ----------

circuits_df = spark.read.parquet(f"{silver_folder_path}/circuits/")

# COMMAND ----------

drivers_df = spark.read.parquet(f"{silver_folder_path}/drivers/")

# COMMAND ----------

constructors_df = spark.read.parquet(f"{silver_folder_path}/constructors/")

# COMMAND ----------

results_df = spark.read.parquet(f"{silver_folder_path}/results/")

# COMMAND ----------

races_circuits_df = ( 
    races_df.join(circuits_df, races_df["circuit_id"] == circuits_df["circuit_id"], "inner")
    .drop(circuits_df.name)        
)

# COMMAND ----------

final_df = (
results_df
.join(constructors_df,results_df["constructor_id"] == constructors_df["constructor_id"],"inner")
.join(drivers_df, results_df["driver_id"] == drivers_df["driver_id"], "inner")
.join(races_circuits_df, results_df["race_id"] == races_circuits_df['race_id'], "inner")
.select(
    races_circuits_df.race_year,
    races_circuits_df.name.alias('race_name'),
    races_circuits_df.date.alias('race_date'),
    races_circuits_df.location.alias('circuit_location'),
    drivers_df.name.alias('driver_name'),
    drivers_df.number.alias('driver_number'),
    drivers_df.nationality.alias('driver_nationality'),
    constructors_df.name.alias('team'),
    results_df.grid,
    results_df.fastest_lap_time.alias("fastest lap"),
    results_df.time.alias("race time"),
    results_df.points,
    results_df.position
    )
.withColumn("created_date",current_timestamp())
)

# COMMAND ----------

display(final_df.filter("race_year == 2020 and race_name == 'Abu Dhabi Grand Prix'").orderBy(final_df.points.desc()))

# COMMAND ----------

final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_gold.race_results")

# COMMAND ----------

