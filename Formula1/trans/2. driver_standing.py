# Databricks notebook source
# MAGIC %md 
# MAGIC
# MAGIC #### Libraries and configuration

# COMMAND ----------

from pyspark.sql.functions import sum, when, count, col
from pyspark.sql.window import Window
from pyspark.sql.functions import desc, rank

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Step 1 - Reading .parquet file

# COMMAND ----------

race_results_df = spark.read.parquet(f"{gold_folder_path}/race_results/")

# COMMAND ----------

# MAGIC
# MAGIC %md
# MAGIC #### Step 2 - Aggregating total_points and wins by race_year, driver_name, driver_nationality, team 

# COMMAND ----------

driver_standings_df = race_results_df \
    .groupBy("race_year","driver_name","driver_nationality","team") \
        .agg(sum("points").alias("total_points"), count(when(col("position") == 1, True)).alias("wins"))

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Step 3 - adding rank column partionating by race_year 

# COMMAND ----------

driver_rank_spec  = Window.partitionBy("race_year").orderBy(desc("total_points"), desc("wins"))
final_df = driver_standings_df.withColumn("rank", rank().over(driver_rank_spec))

# COMMAND ----------

display(final_df.filter("race_year = 2020"))

# COMMAND ----------

final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_gold.driver_standings")

# COMMAND ----------

