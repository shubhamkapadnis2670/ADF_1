# Databricks notebook source
# MAGIC %run "/Workspace/Shubham Kapadnis/connection_info/utility_functions"

# COMMAND ----------

df = spark.readStream.format('cloudFiles').option('cloudFiles.format','csv')\
    .option('cloudFiles.schemaLocation','/dbfs/FileStore/tables/schema/airport')\
    .load('/mnt/raw_datalake/airport/')


# COMMAND ----------

df_base = df.selectExpr(
    "Code",
    "split(Description,',')[0] as city",
    "split(split(Description,',')[1],':')[0] as country",
    "split(split(Description,',')[1],':')[1] as airport",
    "to_date(Date_Part,'yyyy-MM-dd') as Date_Part"

)

df_base.writeStream.trigger(once=True)\
    .format('delta')\
    .option('checkpointLocation','/dbfs/FileStore/tables/checkpointLocation/airport')\
    .start('/mnt/cleansed/airport')

# COMMAND ----------

df = spark.read.format('delta').load('/mnt/cleansed/airport')
schema = f_pre_schema(df)
f_delta_cleansed_load('airport','/mnt/cleansed/airport',schema,'cleansed')

# COMMAND ----------

