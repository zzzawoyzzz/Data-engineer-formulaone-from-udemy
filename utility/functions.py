# Databricks notebook source
# dbutils.notebook.help()

# COMMAND ----------

def pre_schema(df_base):
    schema=""
    for name_and_type in df_base.dtypes:
        schema=schema+name_and_type[0]+' '+name_and_type[1]+',\n'
    return print(schema[:-2])
     

# COMMAND ----------

def create_delta_table(database, table_name, location):
    try:
        spark.sql(f"DROP TABLE IF EXISTS {database}.{table_name}")
        is_partition = dbutils.fs.ls(f'{location}')[1].name
        if is_partition.endswith("/"):
            dbutils.fs.ls(f'{location}')[1].name
            partition_vaule=is_partition.split("=")[0]
            columns = spark.read.format('delta').load(f'{location}').dtypes
            schema = ""
            for name_and_type in columns:
                schema = schema + name_and_type[0] + ' ' + name_and_type[1] + ',\n'
            spark.sql(f"""
                        CREATE TABLE {database}.{table_name}
                        (
                          {schema[:-2]}  
                        )
                        USING DELTA
                        PARTITIONED BY ({partition_vaule})
                        LOCATION '{location}'
                    """)
        else:
            columns = spark.read.format('delta').load(f'{location}').dtypes
            schema = ""
            for name_and_type in columns:
                schema = schema + name_and_type[0] + ' ' + name_and_type[1] + ',\n'
            spark.sql(f"""CREATE OR REPLACE TABLE {database}.{table_name}
                        ({schema[:-2]})  
                        USING DELTA
                        LOCATION '{location}' 
                    """)
    except Exception as err:
        print("Error occurred: ", str(err))

# COMMAND ----------


