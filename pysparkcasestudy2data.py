from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, to_date
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType
import sys

INPUT_PATH = "gs://casestudy2data/realistic_transactions.json"
OUTPUT_PATH = "gs://casestudy2data/processed/"

schema = StructType([
    StructField("store_id", StringType(), True),
    StructField("category_id", StringType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("price", DoubleType(), True),    
    StructField("timestamp", StringType(), True),
    StructField("transaction_id", StringType(), True),
    StructField("customer_id", StringType(), True)
])

if __name__ == "__main__":
    print("PySpark script 'script.py' started.")
    spark = None
    try:
        spark = SparkSession.builder.appName("RetailETL").getOrCreate()
        print("SparkSession initialized successfully.")
        spark.sparkContext.setLogLevel("WARN")
        print(f"Attempting to read JSON data from: {INPUT_PATH}")

        try:
            df = spark.read.option("multiline",True).schema(schema).json(INPUT_PATH)
            print(f"Successfully read data from {INPUT_PATH}.")
            print("\n--- Input DataFrame Schema AFTER reading JSON ---")
            df.printSchema()
            print(f"Number of records read: {df.count()}")

        except Exception as e:
            print(f"ERROR: Failed to read input JSON file from {INPUT_PATH}.", file=sys.stderr)
            print(f"Please check if the file exists and if the Dataproc cluster's service account has 'Storage Object Viewer' permission on the bucket.", file=sys.stderr)
            print(f"Exception details: {e}", file=sys.stderr)
            sys.exit(1)

        if "_corrupt_record" in df.columns and df.filter(col("_corrupt_record").isNotNull()).count() > 0:
            print("CRITICAL WARNING: The input JSON contained corrupt records. Only valid records will be processed.", file=sys.stderr)

        if df.isEmpty() or (len(df.columns) == 1 and "_corrupt_record" in df.columns and df.filter(col("_corrupt_record").isNotNull()).count() == df.count()):
            print("WARNING: Input DataFrame is empty or only contains corrupt records after reading. No valid data to process.", file=sys.stderr)
            sys.exit(0)


        # Clean and transform data
        print("Starting data cleaning and transformation...")
        try:
            df_clean = df.dropna()
            print(f"Number of records after dropna(): {df_clean.count()}")
            df_clean = df_clean.withColumn("total_price", col("quantity") * col("price"))
            print("Added 'total_price' column (quantity * price).")
            df_clean = df_clean.withColumn("transaction_date", to_date(col("timestamp")))
            print("Converted 'timestamp' to 'transaction_date' (DATE type).")
            null_transaction_dates = df_clean.filter(col("transaction_date").isNull()).count()
            if null_transaction_dates > 0:
                print(f"WARNING: {null_transaction_dates} rows have null 'transaction_date' after parsing 'timestamp'. This means 'timestamp' values might be malformed or unparseable.", file=sys.stderr)
        except Exception as e:
            print("ERROR: Data cleaning and transformation failed. This could be due to missing columns, incorrect data types, or invalid data formats in the input JSON (even if schema was provided).", file=sys.stderr)
            print(f"Exception details: {e}", file=sys.stderr)
            df.printSchema()
            sys.exit(1)

        # Aggregate total sales per store, category, and date
        print("Starting data aggregation...")
        try:
            agg_df = df_clean.groupBy("store_id", "category_id", "transaction_date").agg(sum("total_price").alias("total_sales"))
            print("Data aggregation completed.")
            print("Aggregated DataFrame Schema:")
            agg_df.printSchema()
            agg_df.show()
            print(f"Number of aggregated records: {agg_df.count()}")
        except Exception as e:
            print("ERROR: Data aggregation failed. Check group by keys ('store_id', 'category_id', 'transaction_date') and 'total_price' column existence/type.", file=sys.stderr)
            print(f"Exception details: {e}", file=sys.stderr)
            df_clean.printSchema()
            sys.exit(1)

        # Write output to GCS in Parquet format
        print(f"Attempting to write aggregated data to: {OUTPUT_PATH} in Parquet format.")
        try:
            agg_df.write.mode("overwrite").parquet(OUTPUT_PATH)
            print(f"Successfully wrote data to {OUTPUT_PATH}.")
        except Exception as e:
            print(f"ERROR: Failed to write output Parquet files to {OUTPUT_PATH}.", file=sys.stderr)
            print(f"Please check if the GCS path is correct and if the Dataproc cluster's service account has write permissions on the bucket.", file=sys.stderr)
            print(f"Exception details: {e}", file=sys.stderr)
            sys.exit(1)

    except Exception as e:
        print(f"CRITICAL ERROR: An unexpected top-level error occurred in the PySpark script: {e}", file=sys.stderr)
        sys.exit(1)

    finally:
        if spark:
            try:
                spark.stop()
                print("Spark session stopped successfully in finally block.")
            except Exception as e:
                print(f"WARNING: Error stopping Spark session: {e}", file=sys.stderr)
        else:
            print("Spark session was not initialized, skipping stop.")
    print("PySpark script 'script.py' finished.")