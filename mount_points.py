# Databricks notebook source
# Set your storage account name, file system (container) name, and mount point
storage_account_name = "stsourceblob2670"
container_name = "source"
mount_point = "/mnt/blob_source"

# Set your client ID, client secret, directory ID, and tenant ID for Service Principal authentication
client_id = dbutils.secrets.get(scope="azurescope", key= "app-id")
client_secret = dbutils.secrets.get(scope= "azurescope", key= "app-secret-key")
directory_id = "c1732af7-13b0-4802-b087-050fd6569418"


# Define the credentials dictionary
configs = {
    "fs.azure.account.auth.type": "OAuth",
    "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
    "fs.azure.account.oauth2.client.id": client_id,
    "fs.azure.account.oauth2.client.secret": client_secret,
    "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{directory_id}/oauth2/token",
    "fs.azure.createRemoteFileSystemDuringInitialization": "true"
}

# Mount the ADLS Gen2 filesystem
try:
    dbutils.fs.mount(
        source=f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/",
        mount_point=mount_point,
        extra_configs=configs
    )
    print("Blob mounted successfully!")
except Exception as e:
    print(f"Failed to mount blob: {str(e)}")

# COMMAND ----------

# Set your storage account name, file system (container) name, and mount point
storage_account_name = "stsinkadls2670"
container_name = "raw"
mount_point = "/mnt/raw_datalake"

# Set your client ID, client secret, directory ID, and tenant ID for Service Principal authentication
client_id = dbutils.secrets.get(scope="azurescope", key= "app-id")
client_secret = dbutils.secrets.get(scope= "azurescope", key= "app-secret-key")
directory_id = "c1732af7-13b0-4802-b087-050fd6569418"


# Define the credentials dictionary
configs = {
    "fs.azure.account.auth.type": "OAuth",
    "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
    "fs.azure.account.oauth2.client.id": client_id,
    "fs.azure.account.oauth2.client.secret": client_secret,
    "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{directory_id}/oauth2/token",
    "fs.azure.createRemoteFileSystemDuringInitialization": "true"
}

# Mount the ADLS Gen2 filesystem
try:
    dbutils.fs.mount(
        source=f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/",
        mount_point=mount_point,
        extra_configs=configs
    )
    print("ADLS Gen2 mounted successfully!")
except Exception as e:
    print(f"Failed to mount ADLS Gen2: {str(e)}")

# COMMAND ----------

# Set your storage account name, file system (container) name, and mount point
storage_account_name = "stsinkadls2670"
container_name = "cleansed"
mount_point = "/mnt/cleansed"

# Set your client ID, client secret, directory ID, and tenant ID for Service Principal authentication
client_id = dbutils.secrets.get(scope="azurescope", key= "app-id")
client_secret = dbutils.secrets.get(scope= "azurescope", key= "app-secret-key")
directory_id = "c1732af7-13b0-4802-b087-050fd6569418"


# Define the credentials dictionary
configs = {
    "fs.azure.account.auth.type": "OAuth",
    "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
    "fs.azure.account.oauth2.client.id": client_id,
    "fs.azure.account.oauth2.client.secret": client_secret,
    "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{directory_id}/oauth2/token",
    "fs.azure.createRemoteFileSystemDuringInitialization": "true"
}

# Mount the ADLS Gen2 filesystem
try:
    dbutils.fs.mount(
        source=f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/",
        mount_point=mount_point,
        extra_configs=configs
    )
    print("ADLS Gen2 (cleansed) mounted successfully!")
except Exception as e:
    print(f"Failed to mount ADLS Gen2: {str(e)}")



# COMMAND ----------

