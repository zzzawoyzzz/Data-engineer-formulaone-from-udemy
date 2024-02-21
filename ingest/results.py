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

df_results=spark.read\
.schema(schema_results)\
.json('/mnt/source_datalake/raw_from_udemy/results.json')

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
    .save(f"{cleansed__datalake}results")


# COMMAND ----------

display(dbutils.fs.ls('/mnt/formula1/ingest_datalake/results'))

# COMMAND ----------

# MAGIC %sql
# MAGIC show schemas;
# MAGIC use formula_ingest;
# MAGIC show tables;

# COMMAND ----------

#test
def create_delta_table(database, table_name, location):
    try:
        spark.sql(f"DROP TABLE IF EXISTS {database}.{table_name}")
        is_partition = dbutils.fs.ls(f'{location}')[1].name
        if is_partition.endswith("/"):
            dbutils.fs.ls(f'{location}')[1].name
            partition_vaule=is_partition.split("=")[0]
            columns = spark.read.format('delta').load(f'{location}').dtypes
            schema = ""
            for name_and_type in columns:
                schema = schema + name_and_type[0] + ' ' + name_and_type[1] + ',\n'
            spark.sql(f"""
                        CREATE TABLE {database}.{table_name}
                        (
                          {schema[:-2]}  
                        )
                        USING DELTA
                        PARTITIONED BY ({partition_vaule})
                        LOCATION '{location}'
                    """)
        else:
            columns = spark.read.format('delta').load(f'{location}').dtypes
            schema = ""
            for name_and_type in columns:
                schema = schema + name_and_type[0] + ' ' + name_and_type[1] + ',\n'
            spark.sql(f"""CREATE OR REPLACE TABLE {database}.{table_name}
                        ({schema[:-2]})  
                        USING DELTA
                        LOCATION '{location}' 
                    """)
    except Exception as err:
        print("Error occurred: ", str(err))

create_delta_table(database='formula_ingest', table_name='results', location="/mnt/formula1/ingest_datalake/results")

# COMMAND ----------


