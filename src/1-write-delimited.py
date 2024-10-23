# Databricks notebook source
dbutils.widgets.text("delimitedForEach.catalog", "healthverity_claims_sample_patient_dataset")
dbutils.widgets.text("delimitedForEach.schema", "hv_claims_sample")
dbutils.widgets.text("delimitedForEach.table", "procedure")
dbutils.widgets.text("delimitedForEach.maxRowsPerFile", "1000")
dbutils.widgets.text("delimitedForEach.extractVolumePath", "/Volumes/mgiglia/main/extract/delimitedForEach/")
dbutils.widgets.text("delimitedForEach.start", "0")
dbutils.widgets.text("delimitedForEach.stop", "1000")
dbutils.widgets.text("delimitedForEach.file_num", "0")

# COMMAND ----------

catalog_use = dbutils.widgets.get("delimitedForEach.catalog")
schema_use = dbutils.widgets.get("delimitedForEach.schema")
table_use = dbutils.widgets.get("delimitedForEach.table")
max_rows_per_file = int(dbutils.widgets.get("delimitedForEach.maxRowsPerFile"))
extract_path = dbutils.widgets.get("delimitedForEach.extractVolumePath")
record_start = int(dbutils.widgets.get("delimitedForEach.start"))
record_stop = int(dbutils.widgets.get("delimitedForEach.stop"))
file_num = int(dbutils.widgets.get("delimitedForEach.file_num"))

# COMMAND ----------

print(f"""
   catalog_use: {catalog_use}
   schema_use: {schema_use}
   table_use: {table_use}
   max_rows_per_file: {max_rows_per_file}   
   extract_path: {extract_path}
   record_start: {record_start}
   record_stop: {record_stop}
   file_num: {file_num}
""")

# COMMAND ----------

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

display(df)

# COMMAND ----------

(
  df
  .coalesce(1)
  .write
  .mode("overwrite")
  .csv(f"{extract_path}/{file_num}", header=True)
)
