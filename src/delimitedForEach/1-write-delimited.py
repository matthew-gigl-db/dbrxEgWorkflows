# Databricks notebook source
# MAGIC %md
# MAGIC # delimitedForEach
# MAGIC
# MAGIC *** 
# MAGIC ## Write CSV File 
# MAGIC
# MAGIC The purpose of this notebook it to query and filter and table in Unity Catalog to prepare and then write its data to a single CSV file based on the input parameters.  

# COMMAND ----------

# MAGIC %md
# MAGIC ***
# MAGIC
# MAGIC ### Notebook Setup

# COMMAND ----------

# DBTITLE 1,Set Input Parameters
dbutils.widgets.text("delimitedForEach.catalog", "healthverity_claims_sample_patient_dataset", "Catalog")
dbutils.widgets.text("delimitedForEach.schema", "hv_claims_sample", "Schema")
dbutils.widgets.text("delimitedForEach.table", "procedure", "Table")
dbutils.widgets.text("delimitedForEach.maxRowsPerFile", "10000", "Max Rows Per File")
dbutils.widgets.text("delimitedForEach.extractVolumePath", "/Volumes/mgiglia/main/extract/delimitedForEach/", "Extract Volume Path")
dbutils.widgets.text("delimitedForEach.start", "0", "Starting Row")
dbutils.widgets.text("delimitedForEach.stop", "10000", "Stopping Row")
dbutils.widgets.text("delimitedForEach.file_num", "0", "File Number")
dbutils.widgets.text("delimitedForEach.include_header", "false", "Include Header for All Files")

# COMMAND ----------

# DBTITLE 1,Retrieve Input Parameters
catalog_use = dbutils.widgets.get("delimitedForEach.catalog")
schema_use = dbutils.widgets.get("delimitedForEach.schema")
table_use = dbutils.widgets.get("delimitedForEach.table")
max_rows_per_file = int(float(dbutils.widgets.get("delimitedForEach.maxRowsPerFile")))
extract_path = dbutils.widgets.get("delimitedForEach.extractVolumePath")
record_start = int(float(dbutils.widgets.get("delimitedForEach.start")))
record_stop = int(float(dbutils.widgets.get("delimitedForEach.stop")))
file_num = int(float(dbutils.widgets.get("delimitedForEach.file_num")))
include_header_all = dbutils.widgets.get("delimitedForEach.include_header").lower() == 'true'

# COMMAND ----------

# DBTITLE 1,Print Input Parameters
print(f"""
   catalog_use: {catalog_use}
   schema_use: {schema_use}
   table_use: {table_use}
   max_rows_per_file: {max_rows_per_file}   
   extract_path: {extract_path}
   record_start: {record_start}
   record_stop: {record_stop}
   file_num: {file_num}
   include_header_all: {include_header_all}
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ***
# MAGIC ### Query the Delta Table 
# MAGIC
# MAGIC Set the index value and filter to only return the desired records to be written to the CSV file.  

# COMMAND ----------

# DBTITLE 1,Query Dataframe and Filter for Records  Based on Inputs
from pyspark.sql import functions as F
from pyspark.sql.window import Window

df = spark.table(f"{catalog_use}.{schema_use}.{table_use}")
df = df.withColumn(
    'row_index', 
    F.row_number().over(Window.partitionBy(F.lit(1)).orderBy(F.lit(1)))
).filter(
    (F.col('row_index') > record_start) & 
    (F.col('row_index') <= record_stop)
)

# COMMAND ----------

# DBTITLE 1,Optional - Display the Filtered Dataframe
optional_display_df = False

if optional_display_df:
  display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC *** 
# MAGIC ### Write the CSV to an Extract Volume

# COMMAND ----------

# DBTITLE 1,Write the CSV Files
if file_num == 0:
  (
    df
    .coalesce(1)
    .write
    .mode("overwrite")
    .csv(f"{extract_path}/{file_num}", header=True)
  )
else:
  (
    df
    .coalesce(1)
    .write
    .mode("overwrite")
    .csv(f"{extract_path}/{file_num}", header=include_header_all)
  )
