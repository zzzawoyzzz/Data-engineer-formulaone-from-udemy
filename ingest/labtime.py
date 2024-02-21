# Databricks notebook source
# MAGIC %run ../utility/functions

# COMMAND ----------

# MAGIC %run ../configuration/configuration

# COMMAND ----------

display(dbutils.fs.ls(f'{source_datalake}raw_from_udemy/lap_times/'))

# COMMAND ----------

from pyspark.sql.types import StructType,StructField,IntegerType,StringType

schema=StructType(fields=[
    StructField("raceId",IntegerType(),False),
    StructField("driverId",IntegerType(),True),
    StructField("lap",IntegerType(),True),
    StructField("position",IntegerType(),True),
    StructField("time",StringType(),True),
    StructField("milliseconds",IntegerType(),True)
    ])

# COMMAND ----------

df_labtime=spark.read\
    .schema(schema)\
    .option('sep',",")\
    .option("header",True)\
    .csv('/mnt/source_datalake/raw_from_udemy/lap_times/')

display(df_labtime)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp
df_ingest_labtime=df_labtime.withColumnRenamed("raceId","race_id")\
            .withColumnRenamed("driverId","driver_id")\
            .withColumnRenamed("driverId","driver_id")\
            .withColumn("ingestion_date",current_timestamp())
display(df_ingest_labtime)

# COMMAND ----------

df_ingest_labtime.write.format("delta")\
                .mode('overwrite')\
                .save(f'{cleansed__datalake}labtime')

# COMMAND ----------

create_delta_table(database='formula_ingest',table_name='labtime',location='/mnt/formula1/ingest_datalake/labtime/')

# COMMAND ----------


