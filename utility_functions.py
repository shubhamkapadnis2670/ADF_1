# Databricks notebook source
def f_pre_schema(df):
    try:
        schema = ""
        for i in df.dtypes:
            schema = schema + i[0] +" "+i[1] + ","
        return schema[0:-1]
    except Exception as err:
        print('Error occured', str(err))
def f_delta_cleansed_load(table_name,location,schema,database):
    try:

        spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {database}.{table_name}(
            {schema}
        )using delta location '{location}'
                      """)
    except Exception as err:
        print('error occured', str(err))

# COMMAND ----------

