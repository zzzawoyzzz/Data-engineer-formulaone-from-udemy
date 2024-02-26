# Databricks notebook source
# MAGIC %run ../configuration/configuration

# COMMAND ----------

# MAGIC %run ../utility/functions

# COMMAND ----------

display(dbutils.fs.ls("/mnt/formula1/publish_datalake/race_results"))

# COMMAND ----------

drivers_df=spark.read.format("delta").load(f"{cleansed__datalake}drivers")\
    .withColumnRenamed("number","driver_number")\
    .withColumnRenamed("name","driver_name")\
    .withColumnRenamed("nationality","driver_nationality")    

# COMMAND ----------

constructors_df=spark.read.format("delta").load(f"{cleansed__datalake}constructors")\
    .withColumnRenamed("name","team")   

# COMMAND ----------


circuits_df=spark.read.format("delta").load(f"{cleansed__datalake}circuits")\
    .withColumnRenamed("location", "circuit_location") 


# COMMAND ----------


races_df=spark.read.format("delta").load(f"{cleansed__datalake}races")\
    .withColumnRenamed("name", "race_name") \
    .withColumnRenamed("race_timestamp", "race_date") 


# COMMAND ----------


results_df=spark.read.format("delta").load(f"{cleansed__datalake}results")\
.withColumnRenamed("time", "race_time") 


# COMMAND ----------

# MAGIC %md
# MAGIC ##### join table
# MAGIC

# COMMAND ----------

race_circuits_df=circuits_df.join(other=races_df,on=races_df.circuit_id == circuits_df.circuit_id,how="left")\
    .select(races_df.race_id, races_df.race_year, races_df.race_name, races_df.race_date, circuits_df.circuit_location)

# COMMAND ----------

race_results_df = results_df.join(race_circuits_df, results_df.race_id == race_circuits_df.race_id) \
                            .join(drivers_df, results_df.driver_id == drivers_df.driver_id) \
                            .join(constructors_df, results_df.constructor_id == constructors_df.constructor_id)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp
final_df = race_results_df.select("race_year", "race_name", "race_date", "circuit_location", "driver_name", "driver_number", "driver_nationality",
                                 "team", "grid", "fastest_lap", "race_time", "points", "position") \
                          .withColumn("created_date", current_timestamp())

# COMMAND ----------

display(final_df.filter("race_year == 2020 and race_name == 'Abu Dhabi Grand Prix'").orderBy(final_df.points.desc()))

# COMMAND ----------

final_df.write.format('delta').mode("overwrite").save(f"{publish_datalake}/race_results")

# COMMAND ----------

create_delta_table(database='formula_publish',table_name='race_results',location=f"{publish_datalake}/race_results")

# COMMAND ----------


