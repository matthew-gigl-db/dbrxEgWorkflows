# Databricks notebook source
# MAGIC %md
# MAGIC # delimitedForEach
# MAGIC
# MAGIC *** 
# MAGIC ## Combine Files
# MAGIC
# MAGIC The purpose of this notebook is to recombine all of the subfiles into one large file.  This notebook should only be run if the global input paramter to include a "header" on all CSV files was set to "false" otherwise the header record will show up multiple times in the data.  
# MAGIC
# MAGIC Note that if a single large file was needed to be written quickly (and SFTP file size limits didn't exist) then we could have relaxed the coalese = 1 option in the Spark Write CSV instead of using forEach as explictly shown here, performed a similiar rename and then recombined.  Even in this case, forEach should be used for multiple combinations of plan or line of business in a similiar way.  

# COMMAND ----------

# MAGIC %md
# MAGIC ***
# MAGIC
# MAGIC ### Notebook Setup

# COMMAND ----------

# DBTITLE 1,Set Notebook Input Parameters
dbutils.widgets.text("delimitedForEach.extractVolumePath", "/Volumes/mgiglia/main/extract/delimitedForEach/", "Extract Volume Path")
dbutils.widgets.text("delimitedForEach.file_name_prefix", "hedis", "File Name Prefix")

# COMMAND ----------

# DBTITLE 1,Retrieve Notebook Input Parameters
extract_path = dbutils.widgets.get("delimitedForEach.extractVolumePath")
file_name_prefix = dbutils.widgets.get("delimitedForEach.file_name_prefix")

# COMMAND ----------

# DBTITLE 1,Retrieve TaskValues from the Workflow
current_datetime_str = dbutils.jobs.taskValues.get(
    taskKey="0_file_write_range_setup", 
    key="current_datetime_str", 
    debugValue="20241030032724"
)

ranges = dbutils.jobs.taskValues.get(
    taskKey="0_file_write_range_setup", 
    key="ranges", 
    debugValue=[{"start": 0, "stop": 10000, "file_num": 0}, {"start": 0, "stop": 10000, "file_num": 40}]
)

max_file_num = max(range["file_num"] for range in ranges)

# COMMAND ----------

# DBTITLE 1,Print Inputs
print(f"""
   extract_path: {extract_path}  
   file_name_prefix: {file_name_prefix}
   max_file_num: {max_file_num}
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ***
# MAGIC
# MAGIC ### Combine CSV Files Into A Single File 
# MAGIC
# MAGIC Note that only file with `file_num == 0` contains the header record when global workflow paramter `delimitedForEach.include_header` is set to `False`.  

# COMMAND ----------

# DBTITLE 1,Set Directory Path Based on the Current Datetime String
directory_path = f"{extract_path}{current_datetime_str}"
directory_path

# COMMAND ----------

# DBTITLE 1,Combine the Sub Files into a Single File In Order
combined_file_path = f"{directory_path}/{file_name_prefix}_{current_datetime_str}.csv"
with open(combined_file_path, 'wb') as outfile:
    for i in range(max_file_num + 1):
        file_path = f"{directory_path}/{file_name_prefix}_{current_datetime_str}_{i}.csv"
        with open(file_path, 'rb') as infile:
            outfile.write(infile.read())

# COMMAND ----------

# DBTITLE 1,List the Files in the Directory
import subprocess

result = subprocess.run(f"ls -alt {directory_path}", shell=True, capture_output=True)
print(result.stdout.decode('utf-8') + "\n" + result.stderr.decode('utf-8'))

# COMMAND ----------

import os
import gzip
import shutil

for file_name in os.listdir(directory_path):
    file_path = os.path.join(directory_path, file_name)
    if os.path.isfile(file_path):
        with open(file_path, 'rb') as f_in:
            with gzip.open(f"{file_path}.gz", 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)
