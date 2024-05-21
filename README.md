### ReadMe File for NYC Taxi and Limousine Dataset Analysis

## Project Overview

This project ingests the NYC Taxi and Limousine yellow dataset from Azure Blob Storage, summarizes the data by calculating mean, sum of total amounts, and sum of passenger counts aggregated by payment type, year, and month, and outputs the results in both CSV and Parquet formats.

### Updated By: Olivia Gomez
### Updated On: 5/21/2024

## Dependencies

To run this script, you need the following dependencies:

- PySpark
- Databricks (or local Spark setup)
- Azure Blob Storage access

## Installation

1. **Databricks Cluster Setup**:
   - Create a Databricks cluster.
   - Install the necessary libraries in your cluster. You can use the following command in a Databricks notebook cell:
     ```python
     %pip install pyspark
     ```

2. **Local Spark Setup**:
   - Install Apache Spark and set up the necessary environment variables.
   - Install `pyspark`:
     ```bash
     pip install pyspark
     ```

## Azure Blob Storage Configuration

Ensure you have access to the Azure Blob Storage account and container containing the dataset. You will need the following information:

- `blob_account_name`: Azure storage account name.
- `blob_container_name`: Azure storage container name.
- `blob_relative_path`: Path to the dataset within the container.
- `blob_sas_token`: Shared Access Signature (SAS) token for accessing the storage.

## Script Description

The script consists of three main functions:

1. **Ingestion**: Reads the dataset from Azure Blob Storage.
2. **Summarization**: Processes the data to calculate summaries.
3. **Output**: Writes the summarized data to the output directory in CSV and Parquet formats.

### Script

```python
# Updated By: Olivia Gomez
# Updated On: 5/21/2024
# --------------------------

# Azure storage access info
blob_account_name = "azureopendatastorage"
blob_container_name = "nyctlc"
blob_relative_path = "yellow"
blob_sas_token = "r"

# Packages Used
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, month, mean, sum
import os

# Initialize Spark session
spark = SparkSession.builder.appName("NYCTaxiAnalysis").getOrCreate()

def ingestion():
    # Construct the wasbs path
    wasbs_path = f'wasbs://{blob_container_name}@{blob_account_name}.blob.core.windows.net/{blob_relative_path}'
    spark.conf.set(
        f'fs.azure.sas.{blob_container_name}.{blob_account_name}.blob.core.windows.net',
        blob_sas_token
    )
    print('Remote blob path: ' + wasbs_path)

    # Read the Parquet file from the blob storage
    df = spark.read.parquet(wasbs_path)
    df.createOrReplaceTempView('source')
    return df

def summarization(spark_df):
    # Add year and month columns
    spark_df = spark_df.withColumn("puYear", year(col("tpep_pickup_datetime")))
    spark_df = spark_df.withColumn("puMonth", month(col("tpep_pickup_datetime")))

    # Calculate summaries
    summary_df = spark_df.groupBy("payment_type", "puYear", "puMonth")         .agg(
            mean("total_amount").alias("mean_total_amount"),
            sum("total_amount").alias("sum_total_amount"),
            sum("passenger_count").alias("sum_passenger_count")
        )
    return summary_df

def output(df, output_path, format='csv'):
    if format == 'csv':
        df.coalesce(1).write.mode('overwrite').csv(output_path, header=True)
    elif format == 'parquet':
        df.coalesce(1).write.mode('overwrite').parquet(output_path)

if __name__ == "__main__":
    # Ensure the output directory exists
    if not os.path.exists('./Output'):
        os.makedirs('./Output')
    
    # Ingest data
    spark_df = ingestion()

    # Summarize data
    summary_df = summarization(spark_df)

    # Output data
    output_csv_path = './Output/nyc_taxi_summary_csv'
    output_parquet_path = './Output/nyc_taxi_summary_parquet'

    output(summary_df, output_csv_path, format='csv')
    output(summary_df, output_parquet_path, format='parquet')

    print("Data summarization complete. Output saved as CSV and Parquet.")
```

## Running the Script

### In Databricks

1. **Create a New Notebook**:
   - Go to your Databricks workspace and create a new notebook.

2. **Attach the Notebook to a Cluster**:
   - Attach your notebook to the cluster you configured earlier.

3. **Copy and Paste the Script**:
   - Copy the provided script into the notebook cells.

4. **Run the Notebook Cells**:
   - Execute each cell sequentially to run the script.

### Locally

1. **Install Dependencies**:
   - Ensure you have PySpark installed as mentioned in the installation section.

2. **Set Up Azure Blob Storage Access**:
   - Make sure your local environment can access Azure Blob Storage using the provided account name, container name, and SAS token.

3. **Run the Script**:
   - Save the script to a Python file (e.g., `nyc_taxi_analysis.py`).
   - Run the script from your terminal:
     ```bash
     python nyc_taxi_analysis.py
     ```

## Notes

- The script assumes that the dataset is stored in Parquet format in Azure Blob Storage.
- Ensure that the Azure Blob Storage SAS token has the necessary permissions to read the dataset.
- Adjust the Spark configuration and cluster size based on the dataset size and available resources.

By following these instructions, you can successfully run the script and generate the summarized data output in both CSV and Parquet formats.
