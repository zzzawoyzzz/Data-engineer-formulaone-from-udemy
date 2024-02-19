# Databricks notebook source
# MAGIC %md
# MAGIC ### hello
# MAGIC mskks
# MAGIC

# COMMAND ----------

formula1-secret 
source-datalake

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": dbutils.secrets.get(scope="formula1-secret", key="app-client-id"),
          "fs.azure.account.oauth2.client.secret": dbutils.secrets.get(scope="formula1-secret",key="app-secret"),
          "fs.azure.account.oauth2.client.endpoint": dbutils.secrets.get(scope="formula1-secret",key="app-client-endpoint-url")}

# Optionally, you can add <directory-name> to the source URI of your mount point.
dbutils.fs.mount(
  source = dbutils.secrets.get(scope="formula1-secret",key="source-datalake"),
  mount_point = "/mnt/source_datalake",
  extra_configs = configs)

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": dbutils.secrets.get(scope="formula1-secret", key="app-client-id"),
          "fs.azure.account.oauth2.client.secret": dbutils.secrets.get(scope="formula1-secret",key="app-secret"),
          "fs.azure.account.oauth2.client.endpoint": dbutils.secrets.get(scope="formula1-secret",key="app-client-endpoint-url")}

# Optionally, you can add <directory-name> to the source URI of your mount point.
dbutils.fs.mount(
  source = dbutils.secrets.get(scope="formula1-secret",key="ingest-datalake"),
  mount_point = "/mnt/formula1/ingest_datalake",
  extra_configs = configs)

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

display(dbutils.fs.ls('/mnt/formula1/ingest_datalake'))

# COMMAND ----------

dbutils.fs.unmount('/mnt/formula1/ingest_datalake')


# COMMAND ----------


