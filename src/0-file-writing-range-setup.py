# Databricks notebook source
dbutils.widgets.text("delimitedForEach.catalog", "healthverity_claims_sample_patient_dataset")
dbutils.widgets.text("delimitedForEach.schema", "hv_claims_sample")
dbutils.widgets.text("delimitedForEach.table", "procedure")
dbutils.widgets.text("delimitedForEach.maxRowsPerFile", "1000")
dbutils.widgets.text("delimitedForEach.extractVolumePath", "/Volumes/mgiglia/main/extract/delimitedForEach/")

# COMMAND ----------

catalog_use = dbutils.widgets.get("delimitedForEach.catalog")
schema_use = dbutils.widgets.get("delimitedForEach.schema")
table_use = dbutils.widgets.get("delimitedForEach.table")
max_rows_per_file = int(dbutils.widgets.get("delimitedForEach.maxRowsPerFile"))
extract_path = dbutils.widgets.get("delimitedForEach.extractVolumePath")

# COMMAND ----------

print(f"""
   catalog_use: {catalog_use}
   schema_use: {schema_use}
   table_use: {table_use}
   max_rows_per_file: {max_rows_per_file}   
   extract_path: {extract_path}
""")

# COMMAND ----------

df = spark.table(f"{catalog_use}.{schema_use}.{table_use}")
row_count = df.count()
row_count

# COMMAND ----------

ranges = []
start = 0
stop = max_rows_per_file
file_num = 1

while start < row_count:
    ranges.append({
        "catalog": catalog_use,
        "schema": schema_use,
        "table": table_use,
        "start": start,
        "stop": min(stop, row_count),
        "file_num": file_num
    })
    start += max_rows_per_file
    stop += max_rows_per_file
    file_num += 1

ranges

# COMMAND ----------

display(df)

# COMMAND ----------

(
  df
  .coalesce(1)
  .write
  .mode("overwrite")
  .csv(f"{extract_path}/full_file", header=True)
)

# COMMAND ----------

# Read the CSV file written in the last step
df_written = spark.read.csv(f"{extract_path}/full_file", header=True, inferSchema=True)

# Check that the record count is the same as row_count
row_count = df.count()
written_row_count = df_written.count()

assert row_count == written_row_count, f"Row count mismatch: {row_count} != {written_row_count}"

# COMMAND ----------

dbutils.jobs.taskValues.set("ranges", ranges)
