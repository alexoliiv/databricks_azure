# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC #### Ingest races.csv file

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Step 1 - Read the CSV file using the spark dataframe reader

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, DateType
from pyspark.sql.functions import col,current_timestamp,lit,to_timestamp,concat

# COMMAND ----------

circuits_df = spark.read \
    .option("header",True) \
        .option("inferSchema", True) \
            .csv("dbfs:/mnt/formula1dlspalex/raw/races.csv")

# COMMAND ----------

races_schema = StructType(fields=[StructField("raceId"     ,IntegerType(), False),
                                     StructField("year"       ,IntegerType()   , True ),
                                     StructField("round"      ,IntegerType(), True ),
                                     StructField("circuitId"  ,IntegerType(), True ),
                                     StructField("name"       ,StringType() , True ),
                                     StructField("date"       ,DateType()   , True ),
                                     StructField("time"       ,StringType()  , True ),
                                     StructField("url"        ,StringType()  , True )
])

# COMMAND ----------

races_df = spark.read \
    .option("header",True) \
        .schema(races_schema) \
            .csv("dbfs:/mnt/formula1dlspalex/raw/races.csv")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Step 2 - Select only the required columns

# COMMAND ----------

races_selected_df = races_df.select(races_df['raceId'].alias('race_id'),races_df['year'].alias('race_year'),races_df['round'],races_df['circuitId'].alias('circuit_id'),races_df['name'],races_df['date'],races_df['time'])

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Step 3 - Add columns as required

# COMMAND ----------

races_final_df = races_selected_df.withColumn("ingestion_date", current_timestamp()) \
    .withColumn("race_timestamp", to_timestamp(concat(col('date'), lit(' '), col('time')), 'yyyy-MM-dd HH:mm:ss'))

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Step 5 - Write data to datalake as parquet

# COMMAND ----------

races_final_df.write.mode("overwrite").parquet("/mnt/formula1dlspalex/processed/races")

# COMMAND ----------

df = spark.read.parquet("/mnt/formula1dlspalex/processed/races")

# COMMAND ----------


