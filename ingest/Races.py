# Databricks notebook source
# MAGIC %run ../utility/functions

# COMMAND ----------

# MAGIC %run ../configuration/configuration

# COMMAND ----------

df_race=spark.read.option("header",True)\
    .option('inferSchema',True)\
    .option('sep',",")\
    .csv(f'{source_datalake}races.csv')

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
    .save(f"{cleansed__datalake}races")


# COMMAND ----------

create_delta_table(database='formula_ingest',table_name='races',location='/mnt/formula1/ingest_datalake/races/')

# COMMAND ----------

dbutils.fs.ls('/mnt/formula1/ingest_datalake/races/')[1].name.split("=")[0]

# COMMAND ----------

# PARTITIONED BY (race_year='1950', race_year='1951', race_year='1952', race_year='1953', race_year='1954', race_year='1955', race_year='1956', race_year='1957', race_year='1958', race_year='1959', race_year='1960', race_year='1961', race_year='1962', race_year='1963', race_year='1964', race_year='1965', race_year='1966', race_year='1967', race_year='1968', race_year='1969', race_year='1970', race_year='1971', race_year='1972', race_year='1973', race_year='1974', race_year='1975', race_year='1976', race_year='1977', race_year='1978', race_year='1979', race_year='1980', race_year='1981', race_year='1982', race_year='1983', race_year='1984', race_year='1985', race_year='1986', race_year='1987', race_year='1988', race_year='1989', race_year='1990', race_year='1991', race_year='1992', race_year='1993', race_year='1994', race_year='1995', race_year='1996', race_year='1997', race_year='1998', race_year='1999', race_year='2000', race_year='2001', race_year='2002', race_year='2003', race_year='2004', race_year='2005', race_year='2006', race_year='2007', race_year='2008', race_year='2009', race_year='2010', race_year='2011', race_year='2012', race_year='2013', race_year='2014', race_year='2015', race_year='2016', race_year='2017', race_year='2018', race_year='2019', race_year='2020', race_year='2021', race_year='2022', race_year='2023', race_year='2024')
partition_by=""
for folder_name in dbutils.fs.ls('/mnt/formula1/ingest_datalake/races/')[1:]:
    year=str(folder_name[1][:-1].split("=")[1])
    partition_by+=str(folder_name[1][:-1].split("=")[0])+"="+f"'{year}',"
partition_by

# COMMAND ----------

def create_delta_table(database,table_name,location):
  try:
    spark.sql(f"DROP TABLE IF EXISTS {table_name}")
    is_partition=dbutils.fs.ls('/mnt/formula1/ingest_datalake/races/')[1].name
    if is_partition.endswith("/"):
        partitioning_cols =[folder_name[1] for folder_name in dbutils.fs.ls('/mnt/formula1/ingest_datalake/races/')[1:]]
        # partitioning_cols = ",".join(partition_cols)
        # partitioning_clause = f"PARTITIONED BY ({partitioning_cols})"

        partitioning_values = ",".join([f"'{year}'" for year in partitioning_cols])  # Modify this line
        partitioning_clause = f"PARTITIONED BY ({partitioning_cols})" \
                                  f" VALUES ({partitioning_values})"
        
        columns=spark.read.format('delta').load(f'{location}').dtypes
        schema=""
        for name_and_type in columns:
            schema=schema+name_and_type[0]+' '+name_and_type[1]+',\n'
        spark.sql(f"""CREATE OR REPLACE TABLE {database}.{table_name}
                    ({schema[:-2]})  
                    USING DELTA
                    LOCATION '{location}'
                    {partitioning_clause}
                """)   
    else:  
        columns=spark.read.format('delta').load(f'{location}').dtypes
        schema=""
        for name_and_type in columns:
            schema=schema+name_and_type[0]+' '+name_and_type[1]+',\n'
        spark.sql(f""" CREATE OR REPLACE TABLE {database}.{table_name}
                        (
                            {schema[:-2]}
                        )  
                        USING DELTA
                        LOCATION '{location}' 
                """)
  except Exception as err:
      print("error occur :",str(err))

# COMMAND ----------

#test
def create_delta_table(database, table_name, location):
    try:
        spark.sql(f"DROP TABLE IF EXISTS {database}.{table_name}")
        is_partition = dbutils.fs.ls('/mnt/formula1/ingest_datalake/races/')[1].name
        if is_partition.endswith("/"):
            dbutils.fs.ls('/mnt/formula1/ingest_datalake/races/')[1].name
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

create_delta_table(database='formula_ingest', table_name='races', location='/mnt/formula1/ingest_datalake/races/')

# COMMAND ----------

table_name = "races"
database = "formula_ingest"
location = "/mnt/formula1/ingest_datalake/races/"

# Drop the table if it already exists
spark.sql(f"DROP TABLE IF EXISTS {database}.{table_name}")

# Create the table with partitioning
spark.sql(f"""
    CREATE TABLE {database}.{table_name}
    (race_id INT, race_year INT, round INT, circuit_id INT, name STRING, ingestion_date TIMESTAMP, race_timestamp TIMESTAMP)
    USING DELTA
    PARTITIONED BY (race_year)
    LOCATION '{location}'
""")

# COMMAND ----------

# MAGIC %sql 
# MAGIC show schemas;
# MAGIC use formula_ingest;
# MAGIC show tables;

# COMMAND ----------


