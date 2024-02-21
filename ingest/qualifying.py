# Databricks notebook source
# MAGIC %run ../utility/functions

# COMMAND ----------

# MAGIC %run ../configuration/configuration

# COMMAND ----------

display(dbutils.fs.ls(f'{source_datalake}raw_from_udemy/qualifying/'))

# COMMAND ----------

from pyspark.sql.types import StructType,StructField,IntegerType,StringType

qualifying_schema = StructType(fields=[StructField("qualifyId", IntegerType(), False),
                                      StructField("raceId", IntegerType(), True),
                                      StructField("driverId", IntegerType(), True),
                                      StructField("constructorId", IntegerType(), True),
                                      StructField("number", IntegerType(), True),
                                      StructField("position", IntegerType(), True),
                                      StructField("q1", StringType(), True),
                                      StructField("q2", StringType(), True),
                                      StructField("q3", StringType(), True),
                                     ])

# COMMAND ----------

df_qualifying=spark.read\
    .schema(qualifying_schema)\
    .option('multiLine',True)\
    .json('/mnt/source_datalake/raw_from_udemy/qualifying/')


# COMMAND ----------

from pyspark.sql.functions import current_timestamp
df_ingest_qualifying=df_qualifying.withColumnRenamed("qualifyId", "qualify_id") \
                                .withColumnRenamed("driverId", "driver_id") \
                                .withColumnRenamed("raceId", "race_id") \
                                .withColumnRenamed("constructorId", "constructor_id") \
                                .withColumn("ingestion_date", current_timestamp())
display(df_ingest_qualifying)

# COMMAND ----------

df_ingest_qualifying.write.format('delta').mode('overwrite').save(f'{cleansed__datalake}qualifying')

# COMMAND ----------

display(dbutils.fs.ls(f'{cleansed__datalake}qualifying'))

# COMMAND ----------

display(spark.read.load(f'{cleansed__datalake}qualifying'))

# COMMAND ----------

create_delta_table(database='formula_ingest',table_name='qualifying',location='/mnt/formula1/ingest_datalake/qualifying/')

# COMMAND ----------


