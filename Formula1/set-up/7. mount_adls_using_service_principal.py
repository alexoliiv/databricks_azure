# Databricks notebook source
# MAGIC %md
# MAGIC #### Mount Azure Data Lake using Service Principal
# MAGIC 1. Get client_id, tenant_id and client_secret form key vault 
# MAGIC 1. Set Spark Config with App/ Client id, Directory/ Tenant Id & Secret
# MAGIC 1. Call file system utility mount to mount the storage
# MAGIC 1. Explore other file system utilities related to mount (list all mounts, unmount)

# COMMAND ----------

client_id = dbutils.secrets.get(scope='formula1-scope', key = 'formula1-app-client-id')
tenant_id = dbutils.secrets.get(scope='formula1-scope', key = 'formula1-app-tenant-id')
client_secret = dbutils.secrets.get(scope='formula1-scope', key = 'formual1-app-client-secret')

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": client_id,
          "fs.azure.account.oauth2.client.secret": client_secret,
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}

# COMMAND ----------

listContainers = ['demo','raw','processed','presentation']
for container in listContainers:
    CreateMountAzure(configs,container)

# COMMAND ----------

def CreateMountAzure(configs,mount_point):
    lMount = dbutils.fs.mount(
    source = "abfss://mount_point@formula1dlspalex.dfs.core.windows.net/",
    mount_point = f"/mnt/formula1dlspalex/{mount_point}",
    extra_configs = configs)

    if lMount:
        return True
    else:
        return False

# COMMAND ----------

# Optionally, you can add <directory-name> to the source URI of your mount point.
dbutils.fs.mount(
  source = "abfss://demo@formula1dlspalex.dfs.core.windows.net/",
  mount_point = "/mnt/formula1dlspalex/demo",
  extra_configs = configs)

# COMMAND ----------

display(dbutils.fs.ls("/mnt/formula1dlspalex/"))

# COMMAND ----------

display(spark.read.csv("/mnt/formula1dlspalex/demo/circuits.csv"))

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

dbutils.fs.unmount('/mnt/formula1dlspalex/demo')

# COMMAND ----------

