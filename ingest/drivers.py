# Databricks notebook source
# MAGIC %run ../utility/functions

# COMMAND ----------

# MAGIC %run ../configuration/configuration

# COMMAND ----------

display(dbutils.fs.ls(f'{source_datalake}raw_from_udemy/drivers.json'))

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
"number",
"url")


# COMMAND ----------

df_ingest_driver.write.format('delta')\
    .mode('overwrite')\
    .save(f"{cleansed__datalake}drivers")


# COMMAND ----------

create_delta_table(database='formula_ingest',table_name='drivers',location='/mnt/formula1/ingest_datalake/drivers/')
