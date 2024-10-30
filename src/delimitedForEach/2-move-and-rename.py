# Databricks notebook source
dbutils.widgets.text("delimitedForEach.extractVolumePath", "/Volumes/mgiglia/main/extract/delimitedForEach/", "Extract Volume Path")
dbutils.widgets.text("delimitedForEach.file_num", "0", "File Number")

# COMMAND ----------

extract_path = dbutils.widgets.get("delimitedForEach.extractVolumePath")
file_num = int(float(dbutils.widgets.get("delimitedForEach.file_num")))

# COMMAND ----------

current_datetime_str = dbutils.jobs.taskValues.get(
    taskKey="0_file_write_range_setup", 
    key="current_datetime_str", 
    debugValue="20241030015524"
)

# COMMAND ----------

print(f"""
   extract_path: {extract_path}
   file_num: {file_num}
   current_datetime_str: {current_datetime_str}   
""")

# COMMAND ----------

directory_path = f"{extract_path}/{current_datetime_str}/{file_num}/"
directory_path

# COMMAND ----------

import subprocess 

subprocess.run(f"ls -R {directory_path}", shell=True, capture_output=True)

# COMMAND ----------

# Command to find and delete all files and subdirectories not ending in .csv
command = f"find {directory_path} -type f ! -name '*.csv' -delete"

# Execute the command
delete_result = subprocess.run(command, shell=True, capture_output=True)
print(delete_result.stdout.decode("utf-8") + "\n" + delete_result.stderr.decode("utf-8"))

# COMMAND ----------

subprocess.run(f"ls -R {directory_path}", shell=True, capture_output=True)
