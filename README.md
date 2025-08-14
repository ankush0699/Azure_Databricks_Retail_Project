# Azure_Databricks_Retail_Project
This repo contains step by step process of data processing in databricks using medallian architecture.

## **Retail Sales Analytics – Medallion Architecture Pipeline**

### **Project Overview**

This project implements a **Medallion Architecture** (Bronze → Silver → Gold) for retail sales analytics.
The data is ingested from multiple sources, transformed, cleaned, and aggregated in **Azure Databricks**, and finally visualized in **Power BI**.

---

## **Workflow**

<img width="1821" height="421" alt="image" src="https://github.com/user-attachments/assets/17109360-ebd8-404c-8433-8984b76a0da5" />

### **1️⃣ Data Ingestion (Bronze Layer)**

* **Azure Resources Setup**

  * Created an **Azure Storage Account** with containers for `bronze`, `silver`, and `gold` layers.
  * Created an **Azure SQL Database** and tables for structured data storage.

* **Data Source**

  * Retail product, store, customer, and transaction data fetched from **GitHub API** via **Azure Data Factory** pipeline.
  * Data is stored in the **Bronze container** in **Azure Data Lake Storage Gen2** (ADLS) as **Parquet files**.

* **Bronze Layer in Databricks**

  * Configured **OAuth** to securely connect Databricks to ADLS.
  * Read parquet files from `bronze` container into Spark DataFrames for processing.

---

### **2️⃣ Data Transformation (Silver Layer)**

* **Data Cleaning**

  * Converted columns to appropriate data types (e.g., `int`, `date`, `double`).
  * Removed duplicates from customer data.

* **Data Enrichment**

  * Joined all datasets (`transactions`, `customers`, `products`, `stores`) into a single enriched DataFrame.
  * Added `total_amount` column as `quantity * price`.

* **Silver Output**

  * Saved cleaned & enriched dataset in **Delta format** into the `silver` folder in ADLS.

---

### **3️⃣ Data Aggregation (Gold Layer)**

* **Gold Dataset Creation**

  * Loaded cleaned Silver data.
  * Performed aggregations:

    * `total_quantity_sold`
    * `total_sales_amount`
    * `number_of_transactions`
    * `average_transaction_value`

* **Gold Output**

  * Saved aggregated dataset in **Delta format** into the `gold` folder in ADLS.
  * Converted to `gold_delta` folder for proper Delta Lake transaction support.

---

### **4️⃣ Data Registration in Hive Metastore**

* Created a **Hive Metastore** database `retail_data`.
* Registered the Gold dataset as a **managed Delta table**:

  ```sql
  CREATE TABLE IF NOT EXISTS retail_data.final_data_managed
  AS SELECT * FROM delta.`abfss://<container>@<storageaccount>.dfs.core.windows.net/gold/`;
  ```

---

### **5️⃣ Data Visualization in Power BI**

* Created a **SQL Warehouse** in Databricks for Power BI connectivity.
* Connected Power BI to Databricks using:

  * **Server Hostname** (Databricks workspace URL)
  * **HTTP Path** (from SQL Warehouse)
  * **Azure Active Directory Authentication**
* Imported `retail_data.final_data_managed` into Power BI for dashboard creation.
<img width="1322" height="743" alt="image" src="https://github.com/user-attachments/assets/2f097985-a5be-43c0-9d66-9677f65be778" />

---

## **Architecture Diagram**

```text
GitHub API / Azure SQL / Other Sources
        ↓
 Azure Data Factory Pipeline
        ↓
   Bronze Layer (Raw Data in ADLS - Parquet)
        ↓
 Databricks (Cleaning, Type Casting, Joins)
        ↓
   Silver Layer (Enriched Delta Data in ADLS)
        ↓
 Databricks (Aggregation, Metrics Calculation)
        ↓
   Gold Layer (Aggregated Delta Data in ADLS)
        ↓
 Hive Metastore (Managed Delta Table)
        ↓
 Power BI Dashboard (via Databricks SQL Warehouse)
```
