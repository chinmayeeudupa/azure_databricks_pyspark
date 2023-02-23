# Databricks notebook source
storage_account_name = "myformula1dlake"
client_id            = "024fe45d-0d1d-4890-910c-b6670e5f554d"
tenant_id            = "3279bce2-75b5-4b94-8fe1-f62d7ea817fe"
client_secret        = "yXU8Q~tcIEW76.zXEG-JshoEWxT~J8MMwwodra3o"

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
           "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
           "fs.azure.account.oauth2.client.id": f"{client_id}",
           "fs.azure.account.oauth2.client.secret": f"{client_secret}",
           "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}

# COMMAND ----------

container_name = "raw"
dbutils.fs.mount(
  source = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/",
  mount_point = f"/mnt/{storage_account_name}/{container_name}",
  extra_configs = configs)

# COMMAND ----------

dbutils.fs.ls("/mnt/myformula1dlake/raw")

# COMMAND ----------

dbutils.fs.mounts()

# COMMAND ----------

def mount_adls(container_name):
  dbutils.fs.mount(
    source = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/",
    mount_point = f"/mnt/{storage_account_name}/{container_name}",
    extra_configs = configs)

# COMMAND ----------

mount_adls("processed")

# COMMAND ----------

mount_adls("presentation")

# COMMAND ----------

dbutils.fs.ls("/mnt/myformula1dlake/processed")

# COMMAND ----------

dbutils.fs.ls("/mnt/myformula1dlake/presentation")

# COMMAND ----------

dbutils.fs.unmount("/mnt/myformula1dlake/processed")

# COMMAND ----------


