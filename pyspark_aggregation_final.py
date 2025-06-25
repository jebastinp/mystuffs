
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, sum as spark_sum
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

def main():
    input_path = "sample_transactions_with_aggregation.json"
    output_path = "aggregated_output.parquet"

    print("Starting PySpark aggregation script...")

    # Initialize Spark session
    spark = SparkSession.builder.appName("TransactionAggregation").getOrCreate()

    # Define schema
    schema = StructType([
        StructField("transaction_id", StringType(), True),
        StructField("store_id", StringType(), True),
        StructField("category_id", StringType(), True),
        StructField("quantity", IntegerType(), True),
        StructField("price", DoubleType(), True),
        StructField("timestamp", StringType(), True),
        StructField("customer_id", StringType(), True)
    ])

    # Read JSON file with schema
    df = spark.read.schema(schema).json(input_path)
    print("Input data read successfully.")
    df.printSchema()

    # Drop rows with nulls in critical columns
    df_clean = df.dropna(subset=["transaction_id", "store_id", "category_id", "quantity", "price", "timestamp"])

    # Compute total_price
    df_clean = df_clean.withColumn("total_price", col("quantity") * col("price"))

    # Convert timestamp to date
    df_clean = df_clean.withColumn("transaction_date", to_date(col("timestamp")))

    # Aggregate total sales
    df_agg = df_clean.groupBy("store_id", "category_id", "transaction_date") \
                     .agg(spark_sum("total_price").alias("total_sales"))

    print("Aggregation completed. Writing to Parquet...")
    df_agg.write.mode("overwrite").parquet(output_path)
    print(f"Aggregated data written to {output_path}")

    spark.stop()

if __name__ == "__main__":
    main()
