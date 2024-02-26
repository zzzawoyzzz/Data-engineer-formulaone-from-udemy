# Databricks notebook source
import dlt
from pyspark.sql.functions import *

# COMMAND ----------

@dlt.table(
name="Bronze_circuits",
  comment=" ingested from /mnt/source_datalake/raw_from_udemy/circuits.csv",
  table_properties={"layer":"Bronze"}
)
def circuits_ingestion():
  return (
        .format("cloudFiles")
            .option("cloudFiles.format", "json")
            .option("cloudFiles.inferColumnTypes", True)
            .load("/mnt/source_datalake/raw_from_udemy/circuits.csv")
)

# COMMAND ----------

@dlt_table(
    name='silver_circuits',
    comment=" ingested from bronze_df",
    table_properties={"layer":"silver"}
)
def cleansed_circuits():
    return(
        spark.read("Bronze_circuits")
        .option('inferSchema',True)
        .selectExpr("circuitId as circuit_id",
                              "circuitRef as circuit_ref",
                              "name",
                              "location",
                              "country",
                              "lat",
                              "lng",
                              "alt",
                              "url",)
    )

# COMMAND ----------

# display(dbutils.fs.ls("/mnt/source_datalake/raw_from_udemy/"))

# COMMAND ----------


