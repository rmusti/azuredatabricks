# Databricks notebook source
# MAGIC %md
# MAGIC 1. Create Service Principal
# MAGIC    * In Azure Active Directory, go to Properties. Make note of the **Directory ID**.
# MAGIC    * Go to App Registrations and create a New application registration
# MAGIC       * example: airlift-app-registration, Web app/API, https://can-be-literally-anything.com
# MAGIC    * Make note of the **Application ID**.
# MAGIC    * Under Manage >  Certificates & secrets, create and copy a new Client secrets. Make note of the ** Value**.
# MAGIC 1. Create Storage Account
# MAGIC    * On the Advanced Tab (1), make sure to enable Hierarchal NameSpace (2).
# MAGIC       <img src="https://www.evernote.com/l/AAFW89nF7OtKb4j798yshtao-a4SVE2vUk4B/image.png" width=300px>
# MAGIC    * Make note of the **Storage Account Name**.
# MAGIC    * Create a Data Lake Gen2 file system on the storage account and make note of the **File System Name**.
# MAGIC    * Under Access control (IAM) add a *Role assignment*, where the role is *Storage Blob Data Contributor (Preview)* assigned to the App Registration previously created.

# COMMAND ----------


directoryID = "72f988bf-86f1-41af-91ab-2d7cd011db47"
applicationID = "ecf42e9a-b928-4c7c-86e5-5738df90ea0b"
keyValue = "umU2A2Do43.9Rb7Q~9~SOE_BSMFbS-FOEO"
storageAccountName = "fill-me-in"
fileSystemName = "fill-me-in"

configs = {"fs.azure.account.auth.type": "OAuth",
           "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
           "fs.azure.account.oauth2.client.id": applicationID,
           "fs.azure.account.oauth2.client.secret": keyValue,
           "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/{}/oauth2/token".format(directoryID)}


dbutils.fs.mount(
  source = "abfss://{}@{}.dfs.core.windows.net/".format(fileSystemName, storageAccountName),
  mount_point = "/mnt/{}".format(fileSystemName),
  extra_configs = configs)