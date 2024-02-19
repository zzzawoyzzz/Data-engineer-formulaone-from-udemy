# Databricks notebook source
# MAGIC %run ../utility/functions

# COMMAND ----------

display(dbutils.fs.ls('/mnt/source_datalake/raw_from_udemy/constructors.json'))

# COMMAND ----------

df_con=spark.read\
    .schema("""constructorId int,
    constructorRef string,
    name string,
    nationality string,
    url string""")\
    .json('/mnt/source_datalake/raw_from_udemy/constructors.json')

# COMMAND ----------

pre_schema(df_con)

# COMMAND ----------

schema=""
for name_and_type in df_con.dtypes:
    schema=schema+name_and_type[0]+' '+name_and_type[1]+',\n'
print(schema[:-2])


# COMMAND ----------

display(df_con)

# COMMAND ----------

df_conn=df_race.selectExpr("cast(constructorId as int) as constructor_id",
                           "constructorRef as constructor_ref",
                           "name",
                           "nationality"
                           )
display(df_conn)
                            

# COMMAND ----------

df_conn.printSchema()

# COMMAND ----------

df_conn.write.format('delta')\
    .mode('overwrite')\
    .save("/mnt/formula1/ingest_datalake/constructors")


# COMMAND ----------

display(dbutils.fs.ls('/mnt/formula1/ingest_datalake/constructors'))

# COMMAND ----------


