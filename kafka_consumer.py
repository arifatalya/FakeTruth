#!/usr/bin/env python3
"""
Spark Streaming Kafka Consumer + Prediction
============================================
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, concat, lit, lower, regexp_replace,
    when, udf, current_timestamp
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType
)
from pyspark.ml import PipelineModel


# Kafka config
KAFKA_TOPIC = "news-stream"
KAFKA_SERVER = "localhost:9092"

# HDFS paths
HDFS_OUTPUT = "hdfs://localhost:9000/user/news/predictions/streaming"
HDFS_CHECKPOINT = "hdfs://localhost:9000/user/news/checkpoints"
MODEL_PATH = "hdfs://localhost:9000/user/news/model/fake_news_classifier"


def create_spark_session():
    return SparkSession.builder \
        .appName("KafkaNewsStreaming") \
        .config("spark.sql.streaming.checkpointLocation", HDFS_CHECKPOINT) \
        .getOrCreate()


def get_schema():
    """Schema untuk JSON message dari Kafka"""
    return StructType([
        StructField("title", StringType(), True),
        StructField("text", StringType(), True),
        StructField("subject", StringType(), True),
        StructField("date", StringType(), True),
        StructField("timestamp", StringType(), True),
    ])


def preprocess_df(df):
    """Preprocess untuk model prediction"""
    df = df.withColumn("content", concat(col("title"), lit(" "), col("text")))
    df = df.withColumn("content", lower(col("content")))
    df = df.withColumn("content", regexp_replace(col("content"), r"[^a-zA-Z\s]", ""))
    df = df.withColumn("content", regexp_replace(col("content"), r"\s+", " "))
    return df


def main():
    print("="*60)
    print("SPARK STREAMING - KAFKA CONSUMER")
    print("="*60)
    print(f"Kafka Topic: {KAFKA_TOPIC}")
    print(f"Kafka Server: {KAFKA_SERVER}")
    print(f"Output: {HDFS_OUTPUT}")
    
    # Create Spark session
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    
    # Load model
    print(f"\nLoading model from {MODEL_PATH}...")
    try:
        model = PipelineModel.load(MODEL_PATH)
        print("✓ Model loaded!")
    except Exception as e:
        print(f"❌ Error loading model: {e}")
        print("Trying local path...")
        model = PipelineModel.load("/home/payulnaki/fake_news_model")
        print("✓ Model loaded from local!")
    
    # Read from Kafka
    print("\nConnecting to Kafka stream...")
    
    kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_SERVER) \
        .option("subscribe", KAFKA_TOPIC) \
        .option("startingOffsets", "latest") \
        .load()
    
    print("✓ Connected to Kafka!")
    
    # Parse JSON
    schema = get_schema()
    
    parsed_df = kafka_df \
        .selectExpr("CAST(value AS STRING) as json_str") \
        .select(from_json(col("json_str"), schema).alias("data")) \
        .select("data.*")
    
    # Preprocess
    processed_df = preprocess_df(parsed_df)
    
    # UDFs untuk extract probability
    @udf(DoubleType())
    def get_fake_prob(probability):
        try:
            return float(probability[0])
        except:
            return 0.0
    
    @udf(DoubleType())
    def get_true_prob(probability):
        try:
            return float(probability[1])
        except:
            return 0.0
    
    # Process each batch
    def process_batch(batch_df, batch_id):
        if batch_df.count() == 0:
            print(f"Batch {batch_id}: No data")
            return
        
        print(f"\n{'='*60}")
        print(f"BATCH {batch_id} - {batch_df.count()} articles")
        print("="*60)
        
        # Predict
        predictions = model.transform(batch_df)
        
        # Add labels and probabilities
        result = predictions \
            .withColumn("prediction_label", 
                when(col("prediction") == 0.0, "FAKE").otherwise("TRUE")) \
            .withColumn("prob_fake", get_fake_prob(col("probability"))) \
            .withColumn("prob_true", get_true_prob(col("probability"))) \
            .withColumn("processed_at", current_timestamp())
        
        # Select final columns
        output = result.select(
            "title", "text", "subject", "date",
            "prediction_label", "prob_fake", "prob_true", "processed_at"
        )
        
        # Show results
        print("\nPredictions:")
        output.select("title", "subject", "prediction_label", "prob_fake") \
            .show(10, truncate=40)
        
        # Summary
        print("Summary:")
        output.groupBy("prediction_label").count().show()
        
        # Save to HDFS
        output.write \
            .mode("append") \
            .partitionBy("prediction_label") \
            .parquet(HDFS_OUTPUT)
        
        print(f"✅ Saved batch {batch_id} to HDFS")
    
    # Start streaming
    print("\n" + "="*60)
    print("🚀 STREAMING STARTED")
    print("="*60)
    print("Waiting for messages from Kafka...")
    print("Press Ctrl+C to stop\n")
    
    query = processed_df.writeStream \
        .foreachBatch(process_batch) \
        .trigger(processingTime="30 seconds") \
        .start()
    
    try:
        query.awaitTermination()
    except KeyboardInterrupt:
        print("\n\n⛔ Stopping stream...")
        query.stop()
    finally:
        spark.stop()
        print("👋 Spark stopped")


if __name__ == "__main__":
    main()
