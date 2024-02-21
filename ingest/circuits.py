# Databricks notebook source
# MAGIC %run ../utility/functions

# COMMAND ----------

# MAGIC %run ../configuration/configuration

# COMMAND ----------

df_circuit=spark.read.option("header",True)\
    .option('inferSchema',True)\
    .option('sep',",")\
    .csv(f'{source_datalake}circuits.csv')

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
    .save(f"{cleansed__datalake}circuits")


# COMMAND ----------

create_delta_table(database='formula_ingest',table_name='circuits',location='/mnt/formula1/ingest_datalake/circuits/')
