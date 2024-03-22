# Databricks notebook source
# MAGIC %run "/Workspace/Shubham Kapadnis/connection_info/utility_functions"

# COMMAND ----------

df = spark.readStream.format('cloudFiles').option('cloudFiles.format','csv')\
    .option('cloudFiles.schemaLocation','/dbfs/FileStore/tables/schema/PLANE')\
    .load('/mnt/raw_datalake/PLANE/')


# COMMAND ----------

df_base = df.selectExpr('tailnum as tailid','type','manufacturer','to_date(issue_date) as issue_date','model','status','aircraft_type','engine_type','cast(year as int) as year','to_date(Date_Part,"yyyy-MM-dd") as Date_Part')
df_base.writeStream.trigger(once=True)\
    .format('delta')\
    .option('checkpointLocation','/dbfs/FileStore/tables/checkpointLocation/PLANE')\
    .start('/mnt/cleansed/plane')

# COMMAND ----------

df = spark.read.format('delta').load('/mnt/cleansed/plane')
schema = f_pre_schema(df)
f_delta_cleansed_load('plane','/mnt/cleansed/plane',schema,'cleansed')

# COMMAND ----------

