# Databricks notebook source
# MAGIC %run "/Workspace/Shubham Kapadnis/connection_info/utility_functions"

# COMMAND ----------

df = spark.readStream.format('cloudFiles').option('cloudFiles.format','parquet')\
    .option('cloudFiles.schemaLocation','/dbfs/FileStore/tables/schema/cancellation')\
    .load('/mnt/raw_datalake/Cancellation/')


# COMMAND ----------

df_base = df.selectExpr(
    "replace(Code,'\"','') as Code",
    "replace(Description,'\"','') as Description",
    "to_date(Date_Part,'yyyy-MM-dd') as Date_Part"

)

df_base.writeStream.trigger(once=True)\
    .format('delta')\
    .option('checkpointLocation','/dbfs/FileStore/tables/checkpointLocation/cancellation')\
    .start('/mnt/cleansed/cancellation')

# COMMAND ----------

df = spark.read.format('delta').load('/mnt/cleansed/cancellation')
schema = f_pre_schema(df)
f_delta_cleansed_load('cancellation','/mnt/cleansed/cancellation',schema,'cleansed')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from cleansed.cancellation

# COMMAND ----------

