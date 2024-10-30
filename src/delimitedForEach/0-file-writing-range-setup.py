# Databricks notebook source
# MAGIC %md
# MAGIC # delimitedForEach 
# MAGIC
# MAGIC ***  
# MAGIC ## Introduction to the forEach Task 
# MAGIC
# MAGIC The delimitedForEach Workflow is intended to demonstrate the power of the Databricks forEach task for iterating over a parameterized notebook.  
# MAGIC
# MAGIC Many data engineering projects have situations where the same sort of task is required to be repeated over and over again with a parameterized notebook.  Instead of creating separate workflows for each with different parameters, the data engineer will use a Python for loop in a notebook cell to loop over the Databrick's utility's `dbutils.notebook.run` method.  While a clever way to run many versions of the same notebook with different input parameters in a single workflows, it created operational issues for monitoring which of the iterated notebooks run failed, or when a single run of the iterated notebook failed the entire process needed to be completely rerun as the iterations weren't isolated.  
# MAGIC
# MAGIC With the advent of the Databricks Workflow's forEach task, we can now iterate over a notebook's input parameters, have each of the child runs be attached to the parent task and job and it allows an Ops professional the ability to repair the forEach task to only run failed child runs if applicable, saving valuable time and resources.   
# MAGIC
# MAGIC ***  
# MAGIC ## The CSV Delimited File Writing Example 
# MAGIC
# MAGIC A common data engineering task may be to send files to a regulartory body or outside vendor in a specified file formmat.  In the Healthcare & Life Sciences space for example, its very common for payers to use a HEDIS certified partner to prepare their HEDIS submission for NCQA.  These partners often require the extract files to be sent with a specific schema, file format, compression method, file naming convention or file size respectively (e.g. files less than 10GB).  For very large payers with many members this may present a challenge to efficiently write many of these files at once or in succesion with a standard for loop.  
# MAGIC
# MAGIC With the Databricks Workflows' forEach task, the parameterized filters that might be used to write the files (including catalog, schema, and tablename to pull from and the max records per file) can be iterated over with a concurrency value set that optimizes the performance and cost.  It also allows for focused retries of file write tasks that might have failed.  
# MAGIC
# MAGIC ***   
# MAGIC ## Workflow Tasks  
# MAGIC
# MAGIC * **File Writing Range Set Up**: 
# MAGIC   * The input parameters for the Workflow include the catalog, schema, and table name of the table that will be extracted in CSC files.  For sake of the demo we'll assume that this table is the schema required by the vendor.  
# MAGIC   * We'll aslo set the max number of rows that should be written to each file.  For demo purposes we'll set this to 10K records, however for real world scenarios this may likely be in the 10M record range depending on the maximum allowed file size by the vendor typically based on the vendor's SFTP or file submission API requirments (and please talk to your Databricks Account Team about Delta Sharing).  The max number of rows could also be variable based on the number of columns to be written and their average length.  
# MAGIC   * Calculate the number of files to be written, and the row filters or other parameters to be iterated over and store in an array of dictionaries.  Pass this array to TaskValues for the forEach task to use its input. 
# MAGIC * **Iterated CSV File Writes**:  
# MAGIC   * A forEach task that takes the input array from the rnage set up task and runs the parameterized notebook that writes each CSV file to a Unity Catalog Volume using Spark's standard write CSV method.  

# COMMAND ----------

# MAGIC %md
# MAGIC ***
# MAGIC ### Notebook Setup

# COMMAND ----------

# DBTITLE 1,Set Up Notebook Input Parameters
dbutils.widgets.text("delimitedForEach.catalog", "healthverity_claims_sample_patient_dataset", "Catalog")
dbutils.widgets.text("delimitedForEach.schema", "hv_claims_sample", "Schema")
dbutils.widgets.text("delimitedForEach.table", "procedure", "Table")
dbutils.widgets.text("delimitedForEach.maxRowsPerFile", "10000", "Max Rows Per File")
dbutils.widgets.text("delimitedForEach.extractVolumePath", "/Volumes/mgiglia/main/extract/delimitedForEach/", "Extract Volume")
dbutils.widgets.text("delimitedForEach.timezone", "EST", "Timezone for File Creation Timestamp")

# COMMAND ----------

# DBTITLE 1,Retrieve Notebook Input Parameters
catalog_use = dbutils.widgets.get("delimitedForEach.catalog")
schema_use = dbutils.widgets.get("delimitedForEach.schema")
table_use = dbutils.widgets.get("delimitedForEach.table")
max_rows_per_file = int(float(dbutils.widgets.get("delimitedForEach.maxRowsPerFile")))
extract_path = dbutils.widgets.get("delimitedForEach.extractVolumePath")
timezone_for_files = dbutils.widgets.get("delimitedForEach.timezone")

# COMMAND ----------

# DBTITLE 1,Print Notebook Parameters
print(f"""
   catalog_use: {catalog_use}
   schema_use: {schema_use}
   table_use: {table_use}
   max_rows_per_file: {max_rows_per_file}   
   extract_path: {extract_path}
   timezone_for_files: {timezone_for_files}
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ***
# MAGIC ### Datetime Folder for File Writes 
# MAGIC
# MAGIC The files will be written to a subfolder of the extract volume `delimitedForEach/{current_datetime_str}` where the `{current_datetime_str}` takes the form of YYYYMMDDHHMMSS based on the inputted file timezone.  

# COMMAND ----------

# DBTITLE 1,Create the Date Time Stamp  for the File Creation Based on the Timezone Input Parameter
from datetime import datetime
import pytz

current_datetime_str = datetime.now(tz=pytz.timezone(timezone_for_files)).strftime("%Y%m%d%H%M%S")
current_datetime_str

# COMMAND ----------

# MAGIC %md
# MAGIC ***
# MAGIC ### ForEach Task Iteration Setup 
# MAGIC
# MAGIC The forEach Task takes an array as its input to iterate over and feed into the parameterized notebook's Databricks widgets.  If only one input parameter will be iterated over, then a single string array is acceptable.  However sometimes there is more than one input parameter that should be passed.  For this we can use an array of dictionaries where later we'll refer the paramter in the forEach task as `{{input.<dictionary_key>}}` when setting the notebook's inputs.

# COMMAND ----------

# DBTITLE 1,Read Table from Unity Catalog and Calculate the Number of Rows
df = spark.table(f"{catalog_use}.{schema_use}.{table_use}")
row_count = df.count()
row_count

# COMMAND ----------

# DBTITLE 1,Create forEach Input Array Based on Row Count and Max Rows per File
ranges = []
start = 0
stop = max_rows_per_file
file_num = 1

while start < row_count:
    ranges.append({
        "start": start,
        "stop": min(stop, row_count),
        "file_num": file_num
    })
    start += max_rows_per_file
    stop += max_rows_per_file
    file_num += 1

ranges

# COMMAND ----------

# MAGIC %md
# MAGIC ***

# COMMAND ----------

# DBTITLE 1,Optional - Display the Dataframe that will be Written
optional_display_df = False

if optional_display_df:
  display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ***
# MAGIC
# MAGIC ### Optional Full File Write Comparison 
# MAGIC
# MAGIC For very large data frames the forEach method with high concurrency should save the workflow valuable time.  How much time?  Set the below `run_optional_step` to `True` to write a single CSV file.  This demo uses a dataframe that in the grand schema of things is NOT that large, therefore the amount of time used to write many smaller files even with high concurrency is longer.  The purpose of this demo is really to show the power of forEach in the next task.  

# COMMAND ----------

# DBTITLE 1,Optional - Write the Dataframe in a Single File for Time Comparison
run_optional_step = False

if run_optional_step:
  (
    df
    .coalesce(1)
    .write
    .mode("overwrite")
    .csv(f"{extract_path}/full_file", header=True)
  )

# COMMAND ----------

# MAGIC %md
# MAGIC Note that for small tables its more efficient to simply write a single file one time.  For very large files, or for situations where many files are required based on various filters then this forEach method will be a significant time savings.  

# COMMAND ----------

# DBTITLE 1,Optional - Verify Record Counts
if run_optional_step:
  # Read the CSV file written in the last step
  df_written = spark.read.csv(f"{extract_path}/full_file", header=True, inferSchema=True)

  # Check that the record count is the same as row_count
  row_count = df.count()
  written_row_count = df_written.count()

  assert row_count == written_row_count, f"Row count mismatch: {row_count} != {written_row_count}"

# COMMAND ----------

# MAGIC %md
# MAGIC ***
# MAGIC
# MAGIC ### Set TaskValues to Pass Variable Information Between Tasks in a Workflow
# MAGIC
# MAGIC This is where the real magic happens.  We pass the "ranges" array of dictionaries to the forEach iterator's input by using taskValues at the end of this notebook.  Note that you can set many taskValues in a single notebook just like you might set many input parameters.  TaskValues are also retrievable in a notebook using the `dbutils.jobs.taskValues.get(...)` method.  

# COMMAND ----------

# DBTITLE 1,Pass the current_datetime_str to TaskValues for Use in the ForEach Iterator and Recombine Tasks
dbutils.jobs.taskValues.set("current_datetime_str", current_datetime_str)

# COMMAND ----------

# DBTITLE 1,Pass the ranges array to TaskValues for Use in the ForEach Iterator
dbutils.jobs.taskValues.set("ranges", ranges)
