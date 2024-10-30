# Databricks notebook source
dbutils.widgets.text("delimitedForEach.extractVolumePath", "/Volumes/mgiglia/main/extract/delimitedForEach/", "Extract Volume Path")
dbutils.widgets.text("delimitedForEach.file_num", "0", "File Number")
dbutils.widgets.text("delimitedForEach.include_header", "false", "Include Header for All Files")
dbutils.widgets.text("current_datetime_str_debugValue", "", )

# COMMAND ----------



# COMMAND ----------

current_datetime_str =  dbutils.jobs.taskValues.get(
    taskKey="previous_task_name", 
    key="current_datetime_str", 
    debugValue="20241030120000"
)
