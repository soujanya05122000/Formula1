# Databricks notebook source
dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../ConfigAndCommonFunctions/Configuration"

# COMMAND ----------

# MAGIC %run "../ConfigAndCommonFunctions/CommonFunction"

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# COMMAND ----------

lap_times_schema = StructType(fields=[StructField("raceId", IntegerType(), False),
                                      StructField("driverId", IntegerType(), True),
                                      StructField("lap", IntegerType(), True),
                                      StructField("position", IntegerType(), True),
                                      StructField("time", StringType(), True),
                                      StructField("milliseconds", IntegerType(), True)
                                     ])

# COMMAND ----------

lap_times_df = spark.read \
.schema(lap_times_schema) \
.csv(f"{raw_folder_path}/{v_file_date}/lap_times")

# COMMAND ----------

#display(lap_times_df)
#lap_times_df.count()

# COMMAND ----------

from pyspark.sql.functions import lit
LapTimes_final_df = add_ingestion_date(lap_times_df)\
    .withColumnRenamed("driverId", "driver_id") \
    .withColumnRenamed("raceId", "race_id")\
    .withColumn("data_source", lit(v_data_source)) \
    .withColumn("file_date", lit(v_file_date))

# COMMAND ----------

merge_delta_data(
    LapTimes_final_df,
    catalog="azuredatabricks",
    schema="f1_processed",
    table_name="lap_times",
    merge_condition="tgt.race_id = src.race_id AND tgt.driver_id = src.driver_id AND tgt.lap = src.lap",
    partition_column="race_id"
)

# COMMAND ----------

#LapTimes_final_df.write.mode("overwrite").parquet(f"{Processed_folder_path}/lap_times")
#display(spark.read.parquet(f"{Processed_folder_path}/lap_times"))