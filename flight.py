# Databricks notebook source
# MAGIC %run "/Workspace/Shubham Kapadnis/connection_info/utility_functions"

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import *

# COMMAND ----------

flight_schema = StructType([
    StructField('Year', IntegerType(), True), 
    StructField('Month', IntegerType(), True), 
    StructField('DayofMonth', IntegerType(), True), 
    StructField('DayOfWeek', IntegerType(), True), 
    StructField('DepTime', StringType(), True), 
    StructField('CRSDepTime', IntegerType(), True), 
    StructField('ArrTime', StringType(), True), 
    StructField('CRSArrTime', IntegerType(), True), 
    StructField('UniqueCarrier', StringType(), True), 
    StructField('FlightNum', IntegerType(), True), 
    StructField('TailNum', StringType(), True), 
    StructField('ActualElapsedTime', StringType(), True), 
    StructField('CRSElapsedTime', StringType(), True), 
    StructField('AirTime', StringType(), True), 
    StructField('ArrDelay', StringType(), True), 
    StructField('DepDelay', StringType(), True), 
    StructField('Origin', StringType(), True), 
    StructField('Dest', StringType(), True), 
    StructField('Distance', IntegerType(), True), 
    StructField('TaxiIn', StringType(), True), 
    StructField('TaxiOut', StringType(), True), 
    StructField('Cancelled', IntegerType(), True), 
    StructField('CancellationCode', StringType(), True), 
    StructField('Diverted', IntegerType(), True), 
    StructField('CarrierDelay', StringType(), True), 
    StructField('WeatherDelay', StringType(), True), 
    StructField('NASDelay', StringType(), True), 
    StructField('SecurityDelay', StringType(), True), 
    StructField('LateAircraftDelay', StringType(), True), 
    StructField('Date_Part', DateType(), True)
    ])

# COMMAND ----------

df = spark.read.format('csv').option('header',True).schema(schema=flight_schema).load('/mnt/raw_datalake/flight/')
df.write.format('delta').mode('overwrite').save('/mnt/cleansed/flight')

# COMMAND ----------

df = spark.read.format('delta').load('/mnt/cleansed/flight')
schema = f_pre_schema(df)
f_delta_cleansed_load('flight','/mnt/cleansed/flight',schema,'cleansed')

# COMMAND ----------

