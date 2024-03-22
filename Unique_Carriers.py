# Databricks notebook source
# MAGIC %run "/Workspace/Shubham Kapadnis/connection_info/utility_functions"

# COMMAND ----------

df = spark.readStream.format('cloudFiles').option('cloudFiles.format','parquet')\
    .option('cloudFiles.schemaLocation','/dbfs/FileStore/tables/schema/Unique_Carriers')\
    .load('/mnt/raw_datalake/UNIQUE_CARRIERS/')


# COMMAND ----------

df_base = df.selectExpr(
    "replace(Code,'\"','') as Code",
    "replace(Description,'\"','') as Description",
    "to_date(Date_Part,'yyyy-MM-dd') as Date_Part"

)

df_base.writeStream.trigger(once=True)\
    .format('delta')\
    .option('checkpointLocation','/dbfs/FileStore/tables/checkpointLocation/Unique_Carriers')\
    .start('/mnt/cleansed/Unique_Carriers')

# COMMAND ----------

df = spark.read.format('delta').load('/mnt/cleansed/Unique_Carriers')
schema = f_pre_schema(df)
f_delta_cleansed_load('unique_carriers','/mnt/cleansed/Unique_Carriers',schema,'cleansed')