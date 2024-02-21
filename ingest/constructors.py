# Databricks notebook source
# MAGIC %run ../utility/functions

# COMMAND ----------

# MAGIC %run ../configuration/configuration

# COMMAND ----------

display(dbutils.fs.ls(f'{source_datalake}raw_from_udemy/constructors.json'))

# COMMAND ----------

df_con=spark.read\
    .schema("""constructorId int,
    constructorRef string,
    name string,
    nationality string,
    url string""")\
    .json('/mnt/source_datalake/raw_from_udemy/constructors.json')

# COMMAND ----------

display(df_con)

# COMMAND ----------

df_ingest_conn=df_con.selectExpr("cast(constructorId as int) as constructor_id",
                           "constructorRef as constructor_ref",
                           "name",
                           "nationality"
                           )
display(df_ingest_conn)
                            

# COMMAND ----------

df_ingest_conn.write.format('delta')\
    .mode('overwrite')\
    .save(f"{cleansed__datalake}constructors")


# COMMAND ----------

create_delta_table(database='formula_ingest',table_name='constructors',location='/mnt/formula1/ingest_datalake/constructors/')
