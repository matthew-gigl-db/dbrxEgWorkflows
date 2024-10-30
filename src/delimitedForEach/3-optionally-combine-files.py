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
dbutils.widgets.text("delimitedForEach.file_num", "0", "File Number")
dbutils.widgets.text("delimitedForEach.file_name_prefix", "hedis", "File Name Prefix")
dbutils.widgets.text("delimitedForEach.remove_directories", "true", "Remove Directories")

# COMMAND ----------

# DBTITLE 1,Retrieve Notebook Input Parameters
extract_path = dbutils.widgets.get("delimitedForEach.extractVolumePath")
file_num = int(float(dbutils.widgets.get("delimitedForEach.file_num")))
file_name_prefix = dbutils.widgets.get("delimitedForEach.file_name_prefix")
remove_directories = dbutils.widgets.get("delimitedForEach.remove_directories") == "true"

# COMMAND ----------

# DBTITLE 1,Retrieve TaskValues from the Workflow
current_datetime_str = dbutils.jobs.taskValues.get(
    taskKey="0_file_write_range_setup", 
    key="current_datetime_str", 
    debugValue="20241030030404"
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
