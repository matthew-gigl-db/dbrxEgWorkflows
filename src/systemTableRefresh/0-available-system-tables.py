# Databricks notebook source
# DBTITLE 1,Set Databricks Widget for System Catalog Schemas to Exclude from Materializing
dbutils.widgets.text(
    "excluded_schemas",
    "information_schema, __internal_logging, hms_to_uc_migration",
    "Excluded System Schemas",
)

# COMMAND ----------

# DBTITLE 1,Extract Values from Excluded Schemas
# MAGIC %sql
# MAGIC DECLARE OR REPLACE VARIABLE excluded_schemas STRING;
# MAGIC
# MAGIC set variable excluded_schemas = (
# MAGIC   with excluded as (
# MAGIC    select
# MAGIC     trim(explode(split(:excluded_schemas, ','))) AS ex_schema
# MAGIC )
# MAGIC select "'" || CONCAT_WS("', '", COLLECT_LIST(TRIM(ex_schema))) || "'" from excluded
# MAGIC );
# MAGIC
# MAGIC SELECT excluded_schemas;

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
# MAGIC select
# MAGIC   *
# MAGIC from
# MAGIC   tables
# MAGIC where
# MAGIC   table_catalog = 'system'
# MAGIC   and table_schema not in (SELECT explode(trim(split(:excluded_schemas, ','))))
# MAGIC order by
# MAGIC   table_catalog
# MAGIC   ,table_schema
# MAGIC   ,table_name
# MAGIC   ,table_type
# MAGIC   ,data_source_format
# MAGIC ;
# MAGIC

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
