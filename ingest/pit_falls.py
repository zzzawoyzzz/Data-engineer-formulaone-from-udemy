# Databricks notebook source
# MAGIC %run ../utility/functions

# COMMAND ----------

display(dbutils.fs.ls('/mnt/source_datalake/raw_from_udemy/pit_stops.json'))

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
position string,
positionOrder int,
positionText string,
raceId int,
rank string,
resultId int,
statusId int,
time string
"""

# COMMAND ----------

display(spark.read\
.option('multiLine',True)
.json('/mnt/source_datalake/raw_from_udemy/pit_stops.json'))

# COMMAND ----------

name=""
for col_name in df_results.dtypes:
    name=name+f'"{col_name[0]}", \n'
print(name[:-3])

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
    .save("/mnt/formula1/ingest_datalake/results")


# COMMAND ----------

display(dbutils.fs.ls('/mnt/formula1/ingest_datalake/results'))
