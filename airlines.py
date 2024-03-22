# Databricks notebook source
# MAGIC %run "/Workspace/Shubham Kapadnis/connection_info/utility_functions"

# COMMAND ----------

from pyspark.sql.functions import explode, col

# COMMAND ----------

df = spark.read.format('json').load('/mnt/raw_datalake/airlines/')
df1 = df.select(explode('response').alias('response'),'Date_Part')
#df1 = df1.withColumn('iata_code',col('response.iata_code')).withColumn('icao_code',col('response.icao_code')).withColumn('name',col('response.name')).withColumn('Date_Part',col('Date_Part')).drop('response','Date_Part')
df_final = df1.select('response.*','Date_Part')

# COMMAND ----------

df_final.write.format('delta').mode('overwrite').save('/mnt/cleansed/airlines')

# COMMAND ----------

df = spark.read.format('delta').load('/mnt/cleansed/airlines')
schema = f_pre_schema(df)
f_delta_cleansed_load('airlines','/mnt/cleansed/airlines',schema,'cleansed')

# COMMAND ----------

