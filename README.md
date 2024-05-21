## Project Overview

This project ingests the NYC Taxi and Limousine yellow dataset from Azure Blob Storage, summarizes the data by calculating mean, sum of total amounts, and sum of passenger counts aggregated by payment type, year, and month, and outputs the results in both CSV and Parquet formats.

Updated By: Olivia Gomez

Updated On: 5/21/2024

## Dependencies

To run this script, you need the following dependencies:

- PySpark
- Databricks (or local Spark setup)

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

## Script Description

The script consists of three main functions:

1. **Ingestion**: Reads the dataset from Azure Blob Storage.
2. **Summarization**: Processes the data to calculate summaries.
3. **Output**: Writes the summarized data to the output directory in CSV and Parquet formats.

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
