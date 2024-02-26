# Databricks notebook source
# MAGIC %run ../utility/functions

# COMMAND ----------

# MAGIC %run ../configuration/configuration

# COMMAND ----------

# source_datalake="/mnt/source_datalake/"
# cleansed__datalake="/mnt/formula1/ingest_datalake/"

# COMMAND ----------

display(dbutils.fs.ls(f'{source_datalake}raw_from_udemy/results.json'))

# COMMAND ----------

schema_results="""constructorId int,
driverId int,
fastestLap string,
fastestLapSpeed float,
fastestLapTime string,
grid int,
laps int,
milliseconds string,
number string,
points float,
position int,
positionOrder int,
positionText string,
raceId int,
rank int,
resultId int,
statusId int,
time string
"""

# COMMAND ----------

df_results=spark.read\
.schema(schema_results)\
.json('/mnt/source_datalake/raw_from_udemy/results.json')

# COMMAND ----------

display(df_results)

# COMMAND ----------

df_ingest_results=df_results.selectExpr("constructorId as constructor_id", 
"driverId as driver_id", 
"fastestLap as fastest_lap", 
"fastestLapSpeed as fastest_lap_speed", 
"fastestLapTime as fastest_lap_time", 
"grid", 
"laps", 
"milliseconds", 
"number", 
"points", 
"position", 
"positionOrder as position_order", 
"positionText as position_text", 
"raceId as race_id", 
"rank", 
"resultId as result_id", 
"time")

# COMMAND ----------

df_ingest_results.write.format('delta')\
    .partitionBy('race_id')\
    .mode('overwrite')\
    .option("overwriteSchema", "true")\
    .save(f"{cleansed__datalake}results")


# COMMAND ----------

display(dbutils.fs.ls('/mnt/formula1/ingest_datalake/results'))

# COMMAND ----------

create_delta_table(database='formula_ingest',table_name='results',location='/mnt/formula1/ingest_datalake/results')

# COMMAND ----------

# MAGIC %sql
# MAGIC show schemas;
# MAGIC use formula_ingest;
# MAGIC show tables;

# COMMAND ----------


