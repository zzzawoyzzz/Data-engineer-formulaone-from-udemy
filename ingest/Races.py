# Databricks notebook source
# MAGIC %run ../utility/functions

# COMMAND ----------

df_race=spark.read.option("header",True)\
    .option('inferSchema',True)\
    .option('sep',",")\
    .csv('/mnt/source_datalake/races.csv')

# COMMAND ----------

df_ingest=df_race.selectExpr("raceId as race_id",
                                "year as race_year",
                                "round",
                                "circuitId as circuit_id",
                                "name",
                                "current_timestamp as ingestion_date",
                                "to_timestamp(concat(date,' ',time),'yyyy-MM-dd HH:mm:ss') as race_timestamp",)
display(df_ingest)
                            

# COMMAND ----------

df_ingest.write.format('delta')\
    .partitionBy("race_year")\
    .mode('overwrite')\
    .save("/mnt/formula1/ingest_datalake/races")

