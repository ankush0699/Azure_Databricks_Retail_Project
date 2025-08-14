# Databricks notebook source
# MAGIC %md #Medallion Architecture

# COMMAND ----------

# MAGIC %md ###Bronze Layer --Data Ingestion
# MAGIC Here we will fetch data from ADLS into Databricks.

# COMMAND ----------

#Configuring with Azure here.
spark.conf.set("fs.azure.account.auth.type.*****.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.****.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.dbretailproje******ctstorage.dfs.core.windows.net", "******")
spark.conf.set("fs.azure.account.oauth2.client.secret.******.dfs.core.windows.net", "******")
spark.conf.set("fs.azure.account.oauth2.client.endpoint.*****.dfs.core.windows.net", "******/oauth2/token")

# COMMAND ----------

#reading files from bronze folder
df_product = spark.read.option("header", "true").parquet("abfss://dbretailcontainer@dbretailprojectstorage.dfs.core.windows.net/bronze/product/dbo.products.parquet")
df_store = spark.read.option("header", "true").parquet("abfss://dbretailcontainer@dbretailprojectstorage.dfs.core.windows.net/bronze/store/dbo.stores.parquet")
df_customer = spark.read.option("header", "true").parquet("abfss://dbretailcontainer@dbretailprojectstorage.dfs.core.windows.net/bronze/customer/manish040596/azure-data-engineer---multi-source/refs/heads/main/customers.parquet")
df_transaction = spark.read.option("header", "true").parquet("abfss://dbretailcontainer@dbretailprojectstorage.dfs.core.windows.net/bronze/transaction/dbo.transactions.parquet")

# COMMAND ----------

#testing reading of files in bronze
display(df_customer)

# COMMAND ----------

# MAGIC %md ###Silver Layer -- Data Transformation
# MAGIC Here we will do data transformation and cleaning.
# MAGIC

# COMMAND ----------

# Create silver layer - data cleaning
from pyspark.sql.functions import col

# COMMAND ----------

# Converting data types in transactions
df_transaction = df_transaction.select(
    col("transaction_id").cast("int"),
    col("customer_id").cast("int"),
    col("product_id").cast("int"),
    col("store_id").cast("int"),
    col("quantity").cast("int"),
    col("transaction_date").cast("date")
)

# COMMAND ----------

# Converting data types in products
df_product = df_product.select(
    col("product_id").cast("int"),
    col("product_name"),
    col("category"),
    col("price").cast("double"))

# COMMAND ----------

# Converting data types in stores
df_store = df_store.select(
    col("store_id").cast("int"),
    col("store_name"),
    col("location")
)

# COMMAND ----------

# Data cleaning in customers
df_customer = df_customer.select(
    "customer_id", "first_name", "last_name", "email", "city", "registration_date"
).dropDuplicates(["customer_id"])

# COMMAND ----------

# MAGIC %md Joining Tables

# COMMAND ----------

# Join all data together

df_silver = df_transaction \
    .join(df_customer, "customer_id") \
    .join(df_product, "product_id") \
    .join(df_store, "store_id") \
    .withColumn("total_amount", col("quantity") * col("price"))

display(df_silver)

# COMMAND ----------

# MAGIC %md Dumping data of silver layer, i.e; df_silver is now being dumped into ADLS silver folder in container.

# COMMAND ----------

# Dump to ADLS silver folder in container.
silver_path = "abfss://dbretailcontainer@dbretailprojectstorage.dfs.core.windows.net/silver"

df_silver.write.mode("overwrite").format("delta").save(silver_path)

# COMMAND ----------

# MAGIC %md #Gold Layer
# MAGIC Here we will make data in silver layer ready to deploy to client (PowerBI Developer here in this case).

# COMMAND ----------

# Load cleaned transactions from Silver layer
df_silver = spark.read.format("delta").load("abfss://dbretailcontainer@dbretailprojectstorage.dfs.core.windows.net/silver")


# Creating gold dataset by performing aggregation on silver dataset.
from pyspark.sql.functions import sum, countDistinct, avg

gold_df = df_silver.groupBy(
    "transaction_date",
    "product_id", "product_name", "category",
    "store_id", "store_name", "location"
).agg(
    sum("quantity").alias("total_quantity_sold"),
    sum("total_amount").alias("total_sales_amount"),
    countDistinct("transaction_id").alias("number_of_transactions"),
    avg("total_amount").alias("average_transaction_value")
)

display(gold_df)

# COMMAND ----------

# MAGIC %md Now we will dump this gold layer data into gold folder in ADLS inside container.

# COMMAND ----------

#path of gold folder
gold_path = "abfss://dbretailcontainer@dbretailprojectstorage.dfs.core.windows.net/gold"
# dumping in gold folder in ADLS.
gold_df.write.mode("overwrite").format("delta").save(gold_path)

# COMMAND ----------

#converting data from parquet to delta table.
df = spark.read.format("delta").load("abfss://dbretailcontainer@dbretailprojectstorage.dfs.core.windows.net/gold")

df.write.format("delta") \
  .mode("overwrite") \
  .save("abfss://dbretailcontainer@dbretailprojectstorage.dfs.core.windows.net/gold_delta")


# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create database
# MAGIC CREATE DATABASE IF NOT EXISTS retail_data;
# MAGIC --using hive metastore
# MAGIC Use catalog hive_metastore;
# MAGIC --table creration
# MAGIC CREATE TABLE if NOT EXISTS retail_data.final_data_managed
# MAGIC AS SELECT * FROM delta.`abfss://dbretailcontainer@dbretailprojectstorage.dfs.core.windows.net/gold/`;
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from retail_data.final_data_managed

# COMMAND ----------

# MAGIC %md #THE END