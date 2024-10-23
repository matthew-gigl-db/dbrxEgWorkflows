# Databricks notebook source
# DBTITLE 1,Upgrading the Databricks SDK with Pip
# MAGIC %pip install databricks-sdk --upgrade

# COMMAND ----------

# DBTITLE 1,Restart Python
dbutils.library.restartPython()

# COMMAND ----------

# DBTITLE 1,Databricks Workspace Client Initialization
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

# COMMAND ----------

marketplace_listings = w.consumer_listings.list(
  is_free = True
  ,is_private_exchange = False
)
# marketplace_listings = [m.as_dict() for m in marketplace_listings]
marketplace_listings = [m.as_dict() for m in marketplace_listings]

# COMMAND ----------

marketplace_listings

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, ArrayType, LongType

schema = StructType([
    StructField("detail", StructType([
        StructField("assets", ArrayType(StringType()), True),
        StructField("description", StringType(), True),
        StructField("privacy_policy_link", StringType(), True),
        StructField("terms_of_service", StringType(), True)
    ]), True),
    StructField("id", StringType(), True),
    StructField("summary", StructType([
        StructField("categories", ArrayType(StringType()), True),
        StructField("created_at", LongType(), True),
        StructField("listingType", StringType(), True),
        StructField("name", StringType(), True),
        StructField("provider_id", StringType(), True),
        StructField("setting", StructType([
            StructField("visibility", StringType(), True)
        ]), True),
        StructField("share", StructType([
            StructField("name", StringType(), True),
            StructField("type", StringType(), True)
        ]), True),
        StructField("subtitle", StringType(), True),
        StructField("updated_at", LongType(), True)
    ]), True)
])

# Create DataFrame with the defined schema
marketplace_listings_df = spark.createDataFrame(marketplace_listings, schema)
display(marketplace_listings_df)

# COMMAND ----------

from pyspark.sql.functions import col, array_contains

health_table_listings = (
  marketplace_listings_df
  .filter(array_contains(col("detail.assets"), "ASSET_TYPE_DATA_TABLE"))
  .filter(array_contains(col("summary.categories"), "HEALTH"))
  .filter(col("detail.description").contains("claim"))
  .filter(~col("summary.name").contains("[SAMPLE]"))
)

display(health_table_listings)

# COMMAND ----------

listing_id = health_table_listings.filter(col("summary.name") == "Medicare Fee For Service Expenditure Data").select("id").collect()[0][0]

share_name = health_table_listings.filter(col("id") == listing_id).select("summary.share.name").collect()[0][0]

print(f"""
   listing_id: {listing_id}
   share_name: {share_name}   
""")

# COMMAND ----------

installations_list = w.consumer_installations.list_listing_installations(listing_id)
installations_list = [i.as_dict() for i in installations_list]

# COMMAND ----------

installations_list

# COMMAND ----------

if len(installations_list) > 0:
  print("Listing installed") 
else:
  response = w.consumer_installations.create(
    listing_id=listing_id
    ,catalog_name=share_name
    ,share_name=share_name
    ,accepted_consumer_terms=True
  )

# COMMAND ----------


