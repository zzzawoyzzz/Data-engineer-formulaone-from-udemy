# Databricks notebook source
# MAGIC %run ../utility/functions

# COMMAND ----------

display(dbutils.fs.ls('/mnt/source_datalake/raw_from_udemy/drivers.json'))

# COMMAND ----------

schema = """
code string,
dob string,
driverId bigint,
driverRef string,
name struct<forename:string,surname:string>,
nationality string,
number string,
url string
"""
df_driver=spark.read\
.schema(schema)\
.json('/mnt/source_datalake/raw_from_udemy/drivers.json')

# COMMAND ----------

display(df_driver)

# COMMAND ----------

df_ingest_driver=df_driver.selectExpr("code string",
"driverId as driver_id",
"driverRef as driver_ref",
"concat(name.forename,' ',name.surname) as name",
"nationality",
"url")


# COMMAND ----------

df_ingest_driver.write.format('delta')\
    .mode('overwrite')\
    .save("/mnt/formula1/ingest_datalake/drivers")


# COMMAND ----------

display(dbutils.fs.ls('/mnt/formula1/ingest_datalake/drivers'))

# COMMAND ----------


