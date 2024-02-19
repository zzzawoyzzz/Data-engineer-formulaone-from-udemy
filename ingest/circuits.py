# Databricks notebook source
# MAGIC %run ../utility/functions

# COMMAND ----------

df_circuit=spark.read.option("header",True)\
    .option('inferSchema',True)\
    .option('sep',",")\
    .csv('/mnt/source_datalake/circuits.csv')

# COMMAND ----------

df_ingest=df_circuit.selectExpr("circuitId as circuit_id",
                              "circuitRef as circuit_ref",
                              "name",
                              "location",
                              "country",
                              "lat",
                              "lng",
                              "alt",
                              "url",)
display(df_ingest)
                            

# COMMAND ----------

df_ingest.write.format('delta')\
    .mode('overwrite')\
    .save("/mnt/formula1/ingest_datalake/circuits")


# COMMAND ----------


