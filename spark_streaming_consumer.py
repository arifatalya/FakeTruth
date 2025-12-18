from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, to_date, current_timestamp,
    regexp_replace, trim, lower, length, when
)
from pyspark.sql.types import (
    StructType, StructField, StringType, TimestampType
)


# Define schema matching your dataset structure
NEWS_SCHEMA = StructType([
    StructField("title", StringType(), True),
    StructField("text", StringType(), True),
    StructField("subject", StringType(), True),
    StructField("date", StringType(), True),
    StructField("source", StringType(), True),
    StructField("article_id", StringType(), True),
    StructField("scraped_at", StringType(), True)
])


def create_spark_session(app_name: str = "NewsStreamProcessor") -> SparkSession:
    return SparkSession.builder \
        .appName(app_name) \
        .config("spark.jars.packages", 
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
        .config("spark.sql.streaming.checkpointLocation", "/tmp/checkpoints/news") \
        .config("spark.hadoop.fs.defaultFS", "hdfs://localhost:9000") \
        .getOrCreate()


def clean_text(df):
    return df \
        .withColumn("title", trim(regexp_replace(col("title"), r'\s+', ' '))) \
        .withColumn("text", trim(regexp_replace(col("text"), r'\s+', ' '))) \
        .withColumn("text", regexp_replace(col("text"), r'<[^>]+>', '')) \
        .withColumn("subject", lower(trim(col("subject")))) \
        .filter(length(col("text")) > 100)  # Filter out very short articles


def process_news_stream(spark: SparkSession,
                        kafka_servers: str = "localhost:9092",
                        kafka_topic: str = "news-stream",
                        hdfs_path: str = "hdfs://localhost:9000/user/news"):
    """
    Main streaming pipeline:
    1. Read from Kafka
    2. Parse JSON
    3. Clean data
    4. Write to HDFS in format matching your CSV files
    """
    
    # Read from Kafka
    raw_stream = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_servers) \
        .option("subscribe", kafka_topic) \
        .option("startingOffsets", "latest") \
        .option("failOnDataLoss", "false") \
        .load()
    
    # Parse JSON from Kafka value
    parsed_stream = raw_stream \
        .selectExpr("CAST(value AS STRING) as json_str") \
        .select(from_json(col("json_str"), NEWS_SCHEMA).alias("data")) \
        .select("data.*")
    
    # Clean the data
    cleaned_stream = clean_text(parsed_stream)
    
    # Select only columns matching your CSV schema
    final_stream = cleaned_stream.select(
        col("title"),
        col("text"),
        col("subject"),
        col("date"),
        # Additional metadata for your analysis
        col("source"),
        col("article_id"),
        current_timestamp().alias("processed_at")
    )
    
    # Write to HDFS as Parquet (efficient for big data)
    parquet_query = final_stream.writeStream \
        .format("parquet") \
        .option("path", f"{hdfs_path}/parquet") \
        .option("checkpointLocation", f"{hdfs_path}/checkpoints/parquet") \
        .partitionBy("subject", "date") \
        .outputMode("append") \
        .trigger(processingTime="1 minute") \
        .start()
    
    # Also write as CSV for compatibility with your existing data
    csv_query = final_stream.select("title", "text", "subject", "date") \
        .writeStream \
        .format("csv") \
        .option("path", f"{hdfs_path}/csv") \
        .option("checkpointLocation", f"{hdfs_path}/checkpoints/csv") \
        .option("header", "true") \
        .outputMode("append") \
        .trigger(processingTime="5 minutes") \
        .start()
    
    return parquet_query, csv_query


def process_batch_to_hdfs(spark: SparkSession,
                          input_path: str,
                          hdfs_output_path: str):
    
    # Read existing CSV
    df = spark.read \
        .option("header", "true") \
        .option("multiLine", "true") \
        .option("escape", '"') \
        .csv(input_path)
    
    # Clean the data
    cleaned_df = clean_text(df)
    
    # Write to HDFS
    cleaned_df.write \
        .mode("overwrite") \
        .partitionBy("subject") \
        .parquet(f"{hdfs_output_path}/parquet")
    
    # Also save as CSV
    cleaned_df.write \
        .mode("overwrite") \
        .option("header", "true") \
        .csv(f"{hdfs_output_path}/csv")
    
    print(f"Uploaded {cleaned_df.count()} records to HDFS")


def stream_to_console(spark: SparkSession,
                      kafka_servers: str = "localhost:9092",
                      kafka_topic: str = "news-stream"):
    
    raw_stream = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_servers) \
        .option("subscribe", kafka_topic) \
        .option("startingOffsets", "latest") \
        .load()
    
    parsed_stream = raw_stream \
        .selectExpr("CAST(value AS STRING) as json_str") \
        .select(from_json(col("json_str"), NEWS_SCHEMA).alias("data")) \
        .select("data.*")
    
    query = parsed_stream.writeStream \
        .format("console") \
        .option("truncate", "false") \
        .outputMode("append") \
        .start()
    
    return query

if __name__ == "__main__":
    # Create Spark session
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    
    print("Starting News Stream Processor...")
    print("=" * 50)
    
    # Start streaming to HDFS
    parquet_query, csv_query = process_news_stream(
        spark,
        kafka_servers="localhost:9092",
        kafka_topic="news-stream",
        hdfs_path="hdfs://localhost:9000/user/news/streaming"
    )
    
    print("Streaming started!")
    print("Parquet output: hdfs://localhost:9000/user/news/streaming/parquet")
    print("CSV output: hdfs://localhost:9000/user/news/streaming/csv")
    print("=" * 50)
    
    # Wait for termination
    try:
        parquet_query.awaitTermination()
    except KeyboardInterrupt:
        print("\nStopping streams...")
        parquet_query.stop()
        csv_query.stop()
        spark.stop()
