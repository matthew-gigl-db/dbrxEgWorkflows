# Databricks notebook source
# DBTITLE 1,Set Databricks Widget for System Catalog Schemas to Exclude from Materializing
dbutils.widgets.text(
    "excluded_schemas",
    "information_schema, __internal_logging, hms_to_uc_migration",
    "Excluded System Schemas",
)

# COMMAND ----------

# DBTITLE 1,Extract Values from Excluded Schemas and Collect Into a Comment Separated List of Strings
# MAGIC %sql
# MAGIC DECLARE OR REPLACE VARIABLE excluded_schemas STRING;
# MAGIC
# MAGIC set variable excluded_schemas = (
# MAGIC   SELECT "'" || CONCAT_WS("', '", COLLECT_LIST(trim(value))) || "'"
# MAGIC   FROM (
# MAGIC     SELECT explode(split(:excluded_schemas, ',')) AS value
# MAGIC   )
# MAGIC );
# MAGIC
# MAGIC select excluded_schemas;

# COMMAND ----------

# DBTITLE 1,Use the System Catalog's Information_Schema Schema
# MAGIC %sql
# MAGIC use `system`.information_schema;
# MAGIC select current_catalog(), current_schema();

# COMMAND ----------

# DBTITLE 1,Show the Available Tables in system.information Schema
# MAGIC %sql
# MAGIC show tables;

# COMMAND ----------

# DBTITLE 1,Select the table metadata of the System Catalog Tables
# MAGIC %sql
# MAGIC declare or replace variable sql_stmnt string;
# MAGIC
# MAGIC SET VARIABLE sql_stmnt = ("
# MAGIC   select
# MAGIC     *
# MAGIC   from
# MAGIC     tables
# MAGIC   where
# MAGIC     table_catalog = 'system'
# MAGIC     and table_schema not in (" || excluded_schemas || ")
# MAGIC   order by
# MAGIC     table_catalog
# MAGIC     ,table_schema
# MAGIC     ,table_name
# MAGIC     ,table_type
# MAGIC     ,data_source_format
# MAGIC ");
# MAGIC
# MAGIC select sql_stmnt;

# COMMAND ----------

# MAGIC %sql
# MAGIC EXECUTE IMMEDIATE sql_stmnt;

# COMMAND ----------

# DBTITLE 1,Create a list of dictionaries from the Returned Table Metadata
result = _sqldf.collect()
available_system_tables = [
    {
        "table_catalog": row["table_catalog"],
        "table_schema": row["table_schema"],
        "table_name": row["table_name"],
        "comment": row["comment"],
        "storage_sub_directory": row["storage_sub_directory"]
    }
    for row in result
]
available_system_tables

# COMMAND ----------

# MAGIC %sql
# MAGIC describe extended system.access.audit;

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

for table in available_system_tables:
    query = f"""
    DESCRIBE EXTENDED {table['table_catalog']}.{table['table_schema']}.{table['table_name']}
    """
    describe_result = spark.sql(query)
    filtered_result = (
        describe_result.filter(col("col_name").isin("Table Properties", "Location"))
        .select("col_name", "data_type")
        .collect()
    )

    for row in filtered_result:
        if row["col_name"] == "Table Properties":
            table["tblproperties"] = row["data_type"]
        elif row["col_name"] == "Location":
            table["location"] = row["data_type"]

available_system_tables

# COMMAND ----------

dbutils.jobs.taskValues.set("available_system_tables", available_system_tables)
