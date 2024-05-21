# Updated By: Olivia Gomez
# Updated On: 5/21/2024
# --------------------------

# Azure storage access info. This is public information and doesn not need to be encrypted.
blob_account_name = "azureopendatastorage"
blob_container_name = "nyctlc"
blob_relative_path = "yellow"
blob_sas_token = "r"

# Packages Used
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, month, mean, sum
import os

#Initialize Spark session
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
    # Calculate summaries
    summary_df = spark_df.groupBy("paymentType", "puYear", "puMonth") \
        .agg(
            mean("totalAmount").alias("mean_total_amount"),
            sum("totalAmount").alias("sum_total_amount"),
            sum("passengerCount").alias("sum_passenger_count")
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

    output_data(summary_df, output_csv_path, format='csv')
    output_data(summary_df, output_parquet_path, format='parquet')
