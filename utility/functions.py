# Databricks notebook source
def pre_schema(df_base):
    schema=""
    for name_and_type in df_base.dtypes:
        schema=schema+name_and_type[0]+' '+name_and_type[1]+',\n'
    return print(schema[:-2])
     

# COMMAND ----------


