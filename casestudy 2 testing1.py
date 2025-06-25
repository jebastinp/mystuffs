from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, to_date
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
import sys

# --- Configuration ---
INPUT_PATH = "gs://casestudy2testing/sample_transactions.json"
OUTPUT_PATH = "gs://casestudy2testing"

# --- Schema Definition ---
schema = StructType([
    StructField("store_id", StringType(), True),
    StructField("category_id", StringType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("price", DoubleType(), True),
    StructField("timestamp", StringType(), True),
    StructField("transaction_id", StringType(), True),
    StructField("customer_id", StringType(), True)
])

# --- Main Logic ---
if __name__ == "__main__":
    print("PySpark script 'pyspark15.py' started.")

    spark = SparkSession.builder.appName("RetailETL").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    print("SparkSession initialized.")

    df = spark.read.schema(schema).json(INPUT_PATH)
    print("Data read from JSON:")
    df.printSchema()
    print(f"Total records: {df.count()}")

    if "_corrupt_record" in df.columns:
        corrupt_count = df.filter(col("_corrupt_record").isNotNull()).count()
        if corrupt_count > 0:
            print(f"WARNING: {corrupt_count} corrupt records found.")

    if df.rdd.isEmpty():
        print("No valid data to process. Exiting.")
        spark.stop()
        sys.exit(0)

    # --- Data Cleaning & Transformation ---
    df_clean = df.dropna()
    print(f"Records after dropna(): {df_clean.count()}")

    df_clean = df_clean.withColumn("total_price", col("quantity") * col("price"))
    df_clean = df_clean.withColumn("transaction_date", to_date(col("timestamp")))

    null_dates = df_clean.filter(col("transaction_date").isNull()).count()
    if null_dates > 0:
        print(f"WARNING: {null_dates} rows have null 'transaction_date'.")

    # --- Aggregation ---
    agg_df = df_clean.groupBy("store_id", "category_id", "transaction_date") \
                     .agg(sum("total_price").alias("total_sales"))

    print("Aggregated DataFrame:")
    agg_df.printSchema()
    print(f"Aggregated record count: {agg_df.count()}")

    # --- Write Output ---
    agg_df.write.mode("overwrite").parquet(OUTPUT_PATH)
    print(f"Data written to {OUTPUT_PATH} in Parquet format.")

    spark.stop()
    print("Spark session stopped. Script finished.")

