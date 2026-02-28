# Databricks notebook source
dbutils.widgets.text("p_file_date", "2021-03-28")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../ConfigAndCommonFunctions/Configuration"

# COMMAND ----------

# MAGIC %run "../ConfigAndCommonFunctions/CommonFunction"

# COMMAND ----------

race_results_df = spark.table("azuredatabricks.f1_presentation.race_results") \
.filter(f"file_date = '{v_file_date}'") 
#display(race_results_df)

# COMMAND ----------

race_year_list = df_column_to_list(race_results_df, 'race_year')

# COMMAND ----------

from pyspark.sql.functions import col

race_results_df = spark.table("azuredatabricks.f1_presentation.race_results") \
.filter(col("race_year").isin(race_year_list))

# COMMAND ----------

from pyspark.sql.functions import sum,when,col,count
constructor_standings_df = race_results_df \
.groupBy("race_year", "team") \
.agg(sum("points").alias("total_points"),
     count(when(col("position") == 1, True)).alias("wins"))

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import desc, rank, asc

constructor_rank_spec = Window.partitionBy("race_year").orderBy(desc("total_points"), desc("wins"))
final_df = constructor_standings_df.withColumn("rank", rank().over(constructor_rank_spec))
#display(final_df.filter("race_year=2020"))

# COMMAND ----------

merge_delta_data(
    final_df,
    catalog="azuredatabricks",
    schema="f1_presentation",
    table_name="constructor_standings",
    merge_condition="tgt.team = src.team AND tgt.race_year = src.race_year",
    partition_column="race_year"
)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_presentation.constructor_standings WHERE race_year = 2021;

# COMMAND ----------

#merge_condition = "tgt.team = src.team AND tgt.race_year = src.race_year"
#merge_delta_data(final_df, 'f1_presentation', 'constructor_standings', presentation_folder_path, merge_condition, 'race_year')

# COMMAND ----------

#final_df.write.mode("overwrite").parquet(f"{presentation_folder_path}/constructor_standings")