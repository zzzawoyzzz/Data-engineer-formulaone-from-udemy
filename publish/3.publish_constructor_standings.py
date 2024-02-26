# Databricks notebook source
# MAGIC %run ../configuration/configuration

# COMMAND ----------

# MAGIC %run ../utility/functions

# COMMAND ----------

display(dbutils.fs.ls(f"{publish_datalake}"))

# COMMAND ----------

from pyspark.sql.functions import sum, when, count, col

constructor_standings_df=spark.read.format("delta").load(f"{publish_datalake}race_results")\
   .groupBy("race_year", "team") \
    .agg(sum("points").alias("total_points"),
     count(when(col("position") == 1, True)).alias("wins"))

# COMMAND ----------

display(constructor_standings_df.filter("race_year = 2020"))

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import desc, rank, asc

constructor_rank_spec = Window.partitionBy("race_year").orderBy(desc("total_points"), desc("wins"))
final_df = constructor_standings_df.withColumn("rank", rank().over(constructor_rank_spec))

# COMMAND ----------

display(final_df.filter("race_year = 2020"))

# COMMAND ----------

final_df.write.format('delta').mode("overwrite").save(f"{publish_datalake}constructor_standings")

# COMMAND ----------

create_delta_table(database='formula_publish',table_name='constructor_standings',location=f"{publish_datalake}constructor_standings")

# COMMAND ----------


df_row_list = constructor_standings_df.select('race_year') \
                        .distinct() \
                        .collect()
  
column_value_list = [row['race_year'] for row in df_row_list]


# constructor_standings_df=spark.read.format("delta").load(f"{publish_datalake}constructor_standings")
print(column_value_list)

# COMMAND ----------


