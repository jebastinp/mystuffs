from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, sum as spark_sum
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

def main():
    input_path = "gs://casestudy2final/sales_data_final_changed.json"  # Update if needed
    output_path = "gs://casestudy2final"

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

    # ✅ Read JSON file with multiLine option
    df = spark.read.schema(schema).option("multiLine", "true").json(input_path)
    print("Input data read successfully.")
    df.printSchema()
    df.show()

    # Drop rows with nulls in critical columns
    df_clean = df.dropna(subset=["transaction_id", "store_id", "category_id", "quantity", "price", "timestamp"])
    print("Rows after dropna:", df_clean.count())

    # Compute total_price
    df_clean = df_clean.withColumn("total_price", col("quantity") * col("price"))

    # Convert timestamp to date
    df_clean = df_clean.withColumn("transaction_date", to_date(col("timestamp")))

    # Aggregate total sales
    df_agg = df_clean.groupBy("store_id", "category_id", "transaction_date") \
                     .agg(spark_sum("total_price").alias("total_sales"))

    df_agg.show()
    print("Aggregated row count:", df_agg.count())

    # Write to Parquet
    print("Aggregation completed. Writing to Parquet...")
    df_agg.write.mode("overwrite").parquet(output_path)
    print(f"Aggregated data written to {output_path}")

    spark.stop()

if __name__ == "__main__":
    main()
