
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, sum as spark_sum

# Initialize Spark session
spark = SparkSession.builder.appName("SalesAggregation").getOrCreate()

# Read the JSON file
df = spark.read.option("multiline", "true").json("sample_transactions_with_aggregation.json")

# Drop rows with nulls in critical columns
df_clean = df.dropna(subset=["transaction_id", "store_id", "category_id", "quantity", "price", "timestamp"])

# Calculate total_price
df_clean = df_clean.withColumn("total_price", col("quantity") * col("price"))

# Convert timestamp to transaction_date
df_clean = df_clean.withColumn("transaction_date", to_date(col("timestamp")))

# Group and aggregate
aggregated_df = df_clean.groupBy("store_id", "category_id", "transaction_date") \
                         .agg(spark_sum("total_price").alias("total_sales"))

# Write the result to a Parquet file
aggregated_df.write.mode("overwrite").parquet("aggregated_output.parquet")
