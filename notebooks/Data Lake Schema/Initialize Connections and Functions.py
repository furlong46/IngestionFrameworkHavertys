# Databricks notebook source
# MAGIC %scala
# MAGIC //Creates and sets the widgets and variables used later on in the notebook.
# MAGIC dbutils.widgets.text("BulkInsertTableName_Update", "BulkInsertSchemaName.BulkInsertTableName")
# MAGIC val varBulkInsertTableName_Update = dbutils.widgets.get("BulkInsertTableName_Update")
# MAGIC 
# MAGIC dbutils.widgets.text("BulkInsertTableName_Insert", "BulkInsertSchemaName.BulkInsertTableName")
# MAGIC val varBulkInsertTableName_Insert = dbutils.widgets.get("BulkInsertTableName_Insert")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Explanation:
# MAGIC Our notebook starts with setting up connection information to our Data Lake in ADLS, and our Data Warehouse in Azure SQL DB.  Much of the repetive connection info would be templatized or put in a child notebook for code reuse.  

# COMMAND ----------

# MAGIC %md
# MAGIC #### Set the configurations for connecting to ADLS Gen2
# MAGIC Documentation on connecting to ADLS Gen2 is available here: https://docs.databricks.com/spark/latest/data-sources/azure/azure-datalake-gen2.html
# MAGIC All sensitive information is stored using Databricks secrets which can be integrated with Azure Key Vault.  More on secrets here: https://docs.databricks.com/user-guide/secrets/index.html

# COMMAND ----------

varApplicationId = dbutils.secrets.get(scope = "key-vault-secrets", key = "ServicePrincipal21ClientID") #service principle id
varAuthenticationKey = dbutils.secrets.get(scope = "key-vault-secrets", key = "ServicePrincipal21Secret") #service principle key
varTenantId = "804ae74e-8a68-4c11-856b-ccf96b3c8e4e" #the directory id from azure active directory -> properties
varStorageAccountName = "dataanalyticsqaadls21" #storage acccount name
varFileSystemName = "datalake" #ADLS container name

configs = {"fs.azure.account.auth.type": "OAuth",
           "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
           "fs.azure.account.oauth2.client.id": varApplicationId,
           "fs.azure.account.oauth2.client.secret": varAuthenticationKey,
           "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/" + varTenantId + "/oauth2/token"}
try:
  dbutils.fs.mount(
    source = "abfss://" + varFileSystemName + "@" + varStorageAccountName + ".dfs.core.windows.net/",
    mount_point = "/mnt/datalake",
    extra_configs = configs)
except:
  print("{} already mounted".format(varFileSystemName))

# COMMAND ----------

# dbutils.fs.unmount("/mnt/datalake")

# COMMAND ----------

# mounts = dbutils.fs.mounts()

# for mount in mounts:
#   print(mount.mountPoint + " >> " + mount.source)

# print("-"*80)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Set the configurations for the Azure SQL Data DB
# MAGIC Documentation on connecting to Azure SQL DB is available here: https://docs.databricks.com/spark/latest/data-sources/sql-databases.html

# COMMAND ----------

jdbcUsername = "ETL" #The dedicated loading user login 
jdbcPassword = dbutils.secrets.get(scope = "key-vault-secrets", key = "HavertysDWETLAccountPassword") #The dediciated loading user login password

jdbcHostname = dbutils.secrets.get(scope = "key-vault-secrets", key = "HavertysDWServerName") #The Azure SQL Server
jdbcDatabase = dbutils.secrets.get(scope = "key-vault-secrets", key = "HavertysDWDBName") #The Azure SQL Data DB database name
jdbcPort = 1433


jdbcUrl = "jdbc:sqlserver://{0}:{1};database={2}".format(jdbcHostname, jdbcPort, jdbcDatabase)
connectionProperties = {
  "user" : jdbcUsername,
  "password" : jdbcPassword,
  "driver" : "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}  

# COMMAND ----------

# MAGIC %md
# MAGIC Add a user defined MD5 and SHA1 Function - used for creating a hash key for all of the descriptive columns

# COMMAND ----------

import hashlib

def udfMD5Python(colList):
  concatenatedString = ''
  for index, stringValue in enumerate(colList):
    concatenatedString += ('' if stringValue is None else stringValue) # Concatenate all the strings together. Check if stringValue is NULL and insert a blank if true
    if index < len(colList)-1: # Check if its not the last element in the list. If true, then append a '|' to uniquely identify column values
      concatenatedString += '|'
  return hashlib.md5(concatenatedString.encode('utf-8')).hexdigest() # Convert the string to a binary string and then hash the binary string and output a hexidecimal value

spark.udf.register("udfMD5withPython", udfMD5Python)

# COMMAND ----------

import hashlib

def udfSHA1Python(colList):
  concatenatedString = ''
  for index, stringValue in enumerate(colList):
    concatenatedString += ('' if stringValue is None else stringValue) # Concatenate all the strings together. Check if stringValue is NULL and insert a blank if true
    if index < len(colList)-1: # Check if its not the last element in the list. If true, then append a '|' to uniquely identify column values
      concatenatedString += '|'
  return hashlib.sha1(concatenatedString.encode('utf-8')).hexdigest() # Convert the string to a binary string and then hash the binary string and output a hexidecimal value

spark.udf.register("udfSHA1withPython", udfSHA1Python)

# COMMAND ----------

# MAGIC %scala
# MAGIC import com.microsoft.azure.sqldb.spark.bulkcopy.BulkCopyMetadata
# MAGIC import com.microsoft.azure.sqldb.spark.config.Config
# MAGIC import com.microsoft.azure.sqldb.spark.connect._
# MAGIC 
# MAGIC val bulkCopyConfig_Update = Config(Map(
# MAGIC   "url"          -> dbutils.secrets.get(scope = "key-vault-secrets", key = "HavertysDWServerName"),
# MAGIC   "databaseName" -> dbutils.secrets.get(scope = "key-vault-secrets", key = "HavertysDWDBName"),
# MAGIC   "dbTable"      -> varBulkInsertTableName_Update,
# MAGIC   "user"         -> "ETL",
# MAGIC   "password"     -> dbutils.secrets.get(scope = "key-vault-secrets", key = "HavertysDWETLAccountPassword"),
# MAGIC   "bulkCopyBatchSize" -> "10000",
# MAGIC   "bulkCopyTableLock" -> "true",
# MAGIC   "bulkCopyKeepNulls" -> "false",
# MAGIC   "bulkCopyKeepIdentity" -> "false",
# MAGIC   "bulkCopyTimeout"   -> "600"
# MAGIC ))

# COMMAND ----------

# MAGIC %scala
# MAGIC import com.microsoft.azure.sqldb.spark.bulkcopy.BulkCopyMetadata
# MAGIC import com.microsoft.azure.sqldb.spark.config.Config
# MAGIC import com.microsoft.azure.sqldb.spark.connect._
# MAGIC 
# MAGIC val bulkCopyConfig_Insert = Config(Map(
# MAGIC   "url"          -> dbutils.secrets.get(scope = "key-vault-secrets", key = "HavertysDWServerName"),
# MAGIC   "databaseName" -> dbutils.secrets.get(scope = "key-vault-secrets", key = "HavertysDWDBName"),
# MAGIC   "dbTable"      -> varBulkInsertTableName_Insert,
# MAGIC   "user"         -> "ETL",
# MAGIC   "password"     -> dbutils.secrets.get(scope = "key-vault-secrets", key = "HavertysDWETLAccountPassword"),
# MAGIC   "bulkCopyBatchSize" -> "10000",
# MAGIC   "bulkCopyTableLock" -> "true",
# MAGIC   "bulkCopyKeepNulls" -> "false",
# MAGIC   "bulkCopyKeepIdentity" -> "false",
# MAGIC   "bulkCopyTimeout"   -> "600"
# MAGIC ))