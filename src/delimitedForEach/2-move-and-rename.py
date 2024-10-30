# Databricks notebook source
# MAGIC %md
# MAGIC # delimitedForEach
# MAGIC
# MAGIC *** 
# MAGIC ## Move and Rename Files
# MAGIC
# MAGIC The purpose of this notebook is to reach into each subdirectory and identify the written CSV file, copy it the parent directory `delimitedForEach/{current_datetime_str}` and rename the file based on the `file_name_prefix` the `current_datetime_str` and the `file_num`.     

# COMMAND ----------

# MAGIC %md
# MAGIC ***
# MAGIC
# MAGIC ### Notebook Setup

# COMMAND ----------

# DBTITLE 1,Set Notebook Input Parameters
dbutils.widgets.text("delimitedForEach.extractVolumePath", "/Volumes/mgiglia/main/extract/delimitedForEach/", "Extract Volume Path")
dbutils.widgets.text("delimitedForEach.file_num", "0", "File Number")
dbutils.widgets.text("delimitedForEach.file_name_prefix", "hedis", "File Name Prefix")
dbutils.widgets.text("delimitedForEach.remove_directories", "True", "Remove Directories")

# COMMAND ----------

# DBTITLE 1,Retrieve Notebook Input Parameters
extract_path = dbutils.widgets.get("delimitedForEach.extractVolumePath")
file_num = int(float(dbutils.widgets.get("delimitedForEach.file_num")))
file_name_prefix = dbutils.widgets.get("delimitedForEach.file_name_prefix")
remove_directories = dbutils.widgets.get("delimitedForEach.remove_directories") == "True"

# COMMAND ----------

# DBTITLE 1,Retrieve TaskValues from the Workflow
current_datetime_str = dbutils.jobs.taskValues.get(
    taskKey="0_file_write_range_setup", 
    key="current_datetime_str", 
    debugValue="20241030022002"
)

# COMMAND ----------

# DBTITLE 1,Print Inputs
print(f"""
   extract_path: {extract_path}
   file_num: {file_num}
   current_datetime_str: {current_datetime_str}   
   file_name_prefix: {file_name_prefix}
   remove_directories: {remove_directories}
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ***
# MAGIC
# MAGIC ### Copy and Rename Delimited Files

# COMMAND ----------

# DBTITLE 1,Set the Directory Path to Look for Delimited Files
directory_path = f"{extract_path}/{current_datetime_str}/{file_num}/"
directory_path

# COMMAND ----------

# DBTITLE 1,List Files in the Directory
import subprocess 

ls_result = subprocess.run(f"ls -R {directory_path}", shell=True, capture_output=True)
print(ls_result.stdout.decode("utf-8") + "\n" + ls_result.stderr.decode("utf-8"))

# COMMAND ----------

# DBTITLE 1,Remove Non-CSV Files (and potential Spark Holds)
# Command to find and delete all files and subdirectories not ending in .csv
command = f"find {directory_path} -type f ! -name '*.csv' -delete"

# Execute the command
delete_result = subprocess.run(command, shell=True, capture_output=True)
print(delete_result.stdout.decode("utf-8") + "\n" + delete_result.stderr.decode("utf-8"))

# COMMAND ----------

# DBTITLE 1,List Remaining Files in the Directory
ls_result = subprocess.run(f"ls -R {directory_path}", shell=True, capture_output=True)
print(ls_result.stdout.decode("utf-8") + "\n" + ls_result.stderr.decode("utf-8"))

# COMMAND ----------

# DBTITLE 1,Copy and Rename Delimited File to Parent Directory
# List all CSV files in the directory
list_csv_command = f"find {directory_path} -type f -name '*.csv'"
csv_files = subprocess.run(list_csv_command, shell=True, capture_output=True, text=True).stdout.splitlines()

# Move and rename each CSV file
for file_num, csv_file in enumerate(csv_files, start=1):
    new_file_name = f"{file_name_prefix}_{current_datetime_str}_{file_num}.csv"
    move_command = f"cp -f {csv_file} {directory_path}/../{new_file_name}"
    subprocess.run(move_command, shell=True)

# COMMAND ----------

# DBTITLE 1,List Delimited Files in the Parent Directory
ls_result = subprocess.run(f"find {directory_path}/../ -maxdepth 1 -type f", shell=True, capture_output=True)
print(ls_result.stdout.decode("utf-8") + "\n" + ls_result.stderr.decode("utf-8"))

# COMMAND ----------

# MAGIC %md
# MAGIC ***
# MAGIC
# MAGIC ### Clean Up 
# MAGIC
# MAGIC Optionally remove extraneous files and directories from the volume.  

# COMMAND ----------

# DBTITLE 1,Optionally Delete the Original Directories
if remove_directories:
  # Remove the directory_path directory
  remove_directory_command = f"rm -rf {directory_path}"
  subprocess.run(remove_directory_command, shell=True)
