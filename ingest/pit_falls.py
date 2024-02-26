# Databricks notebook source
# MAGIC %run ../utility/functions

# COMMAND ----------

# MAGIC %run ../configuration/configuration

# COMMAND ----------

display(dbutils.fs.ls(f'{source_datalake}raw_from_udemy/pit_stops.json'))

# COMMAND ----------

from pyspark.sql.types import StructType,StructField,IntegerType,StringType
pit_stops_schema=StructType(fields=[StructField(name='driverId',dataType=IntegerType(),nullable=False),
                                    StructField(name='duration',dataType=StringType(),nullable=True),
                                    StructField(name='lap',dataType=IntegerType(),nullable=True),
                                    StructField(name='milliseconds',dataType=IntegerType(),nullable=True),
                                    StructField(name='raceId',dataType=IntegerType(),nullable=True),
                                    StructField(name='stop',dataType=IntegerType(),nullable=True),
                                    StructField(name='time',dataType=StringType(),nullable=True)
                                    ]
                            )                            


# COMMAND ----------

df_pit_fall=spark.read\
.schema(pit_stops_schema)\
.option('multiLine',True)\
.json('/mnt/source_datalake/raw_from_udemy/pit_stops.json')

# COMMAND ----------

from pyspark.sql.functions import col,current_timestamp

df_ingest_pit_fall=df_pit_fall.withColumnRenamed("driverId","driver_id")\
.withColumnRenamed("raceId","race_id")\
.withColumn("current_timestamp",current_timestamp())



# COMMAND ----------

display(df_ingest_pit_fall)

# COMMAND ----------

df_ingest_pit_fall.write.format('delta')\
    .mode('overwrite')\
    .save(f"{cleansed__datalake}pit_falls")

# COMMAND ----------

create_delta_table(database='formula_ingest',table_name='pit_falls',location='/mnt/formula1/ingest_datalake/pit_falls/')

# COMMAND ----------


