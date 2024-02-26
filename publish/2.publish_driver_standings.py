# Databricks notebook source
# MAGIC %run ../configuration/configuration

# COMMAND ----------

# MAGIC %run ../utility/functions

# COMMAND ----------

display(dbutils.fs.ls(f"{publish_datalake}"))

# COMMAND ----------

from pyspark.sql.functions import sum, when, count, col

driver_standings_df=spark.read.format("delta").load(f"{publish_datalake}race_results")\
    .groupBy('race_year','race_name','driver_name','driver_nationality','team')\
    .agg(sum('points').alias("total_points"),
    count(when(col("position") == 1, True)).alias("wins"))

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import desc, rank, asc

driver_rank_spec = Window.partitionBy("race_year").orderBy(desc("total_points"), desc("wins"))
final_df = driver_standings_df.withColumn("rank", rank().over(driver_rank_spec))

# COMMAND ----------

display(final_df.filter("race_year = 2020"))

# COMMAND ----------

final_df.write.format('delta').mode("overwrite").save(f"{publish_datalake}driver_standings")

# COMMAND ----------

create_delta_table(database='formula_publish',table_name='driver_standings',location=f"{publish_datalake}driver_standings")
