# Databricks notebook source
dbutils.widgets.text("delimitedForEach.extractVolumePath", "/Volumes/mgiglia/main/extract/delimitedForEach/", "Extract Volume Path")
dbutils.widgets.text("delimitedForEach.file_num", "0", "File Number")
dbutils.widgets.text("delimitedForEach.include_header", "false", "Include Header for All Files")

# COMMAND ----------

extract_path = dbutils.widgets.get("delimitedForEach.extractVolumePath")
file_num = int(float(dbutils.widgets.get("delimitedForEach.file_num")))
include_header_all = dbutils.widgets.get("delimitedForEach.include_header").lower() == 'true'

# COMMAND ----------

current_datetime_str = dbutils.jobs.taskValues.get(
    taskKey="file_write_range_setup", 
    key="current_datetime_str", 
    debugValue="20241031115959"
)

# COMMAND ----------

print(f"""
   extract_path: {extract_path}
   file_num: {file_num}
   include_header_all: {include_header_all}
   current_datetime_str: {current_datetime_str}   
""")
