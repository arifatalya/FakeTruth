#!/usr/bin/env python3
"""
Export HDFS Predictions to CSV for Grafana
==========================================
Export data dari HDFS ke single CSV file

Usage:
    spark-submit export_to_csv.py
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, when, lit, date_format, to_timestamp
import os
import shutil
from datetime import datetime


def create_spark_session():
    return SparkSession.builder \
        .appName("ExportToCSV") \
        .getOrCreate()


def export_predictions(spark, hdfs_path, output_dir):
    """Export predictions dari HDFS ke CSV"""
    
    print(f"Reading from: {hdfs_path}")
    
    try:
        df = spark.read.parquet(hdfs_path)
        total = df.count()
        print(f"✓ Loaded {total} records")
    except Exception as e:
        print(f"❌ Error reading HDFS: {e}")
        return None
    
    # Show summary
    print("\n📊 Data Summary:")
    df.groupBy("prediction_label").count().show()
    
    print("By Subject:")
    df.groupBy("subject", "prediction_label").count().orderBy("subject").show(20)
    
    # Create output directory
    os.makedirs(output_dir, exist_ok=True)
    
    # Temp path for Spark output
    temp_path = f"{output_dir}/temp"
    
    # Export main predictions
    print(f"\nExporting to {output_dir}/predictions.csv ...")
    
    df.select(
        "title", "subject", "date", 
        "prediction_label", "prob_fake", "prob_true"
    ).coalesce(1) \
        .write \
        .mode("overwrite") \
        .option("header", "true") \
        .csv(temp_path)
    
    # Move and rename to single CSV
    for f in os.listdir(temp_path):
        if f.endswith(".csv"):
            shutil.move(f"{temp_path}/{f}", f"{output_dir}/predictions.csv")
            break
    
    # Cleanup temp
    shutil.rmtree(temp_path, ignore_errors=True)
    
    print(f"✓ Saved: {output_dir}/predictions.csv")
    
    # Create summary CSV for Grafana charts
    print(f"\nCreating summary CSVs...")
    
    # Summary by prediction label
    summary_label = df.groupBy("prediction_label").count().toPandas()
    summary_label.to_csv(f"{output_dir}/summary_by_label.csv", index=False)
    print(f"✓ Saved: {output_dir}/summary_by_label.csv")
    
    # Summary by subject
    summary_subject = df.groupBy("subject", "prediction_label") \
        .count() \
        .orderBy("subject") \
        .toPandas()
    summary_subject.to_csv(f"{output_dir}/summary_by_subject.csv", index=False)
    print(f"✓ Saved: {output_dir}/summary_by_subject.csv")
    
    # Summary by date
    summary_date = df.groupBy("date", "prediction_label") \
        .count() \
        .orderBy("date") \
        .toPandas()
    summary_date.to_csv(f"{output_dir}/summary_by_date.csv", index=False)
    print(f"✓ Saved: {output_dir}/summary_by_date.csv")
    
    return total


def main():
    print("="*50)
    print("EXPORT HDFS TO CSV FOR GRAFANA")
    print("="*50)
    
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    
    # Paths
    hdfs_predictions = "hdfs://localhost:9000/user/news/predictions/streaming"
    output_dir = "/home/payulnaki/grafana_data"
    
    try:
        count = export_predictions(spark, hdfs_predictions, output_dir)
        
        if count:
            print("\n" + "="*50)
            print("✅ EXPORT COMPLETE!")
            print("="*50)
            print(f"Total records: {count}")
            print(f"Output folder: {output_dir}")
            print(f"\nFiles created:")
            print(f"  - predictions.csv (all data)")
            print(f"  - summary_by_label.csv (FAKE vs TRUE)")
            print(f"  - summary_by_subject.csv (by category)")
            print(f"  - summary_by_date.csv (by date)")
            print("\nNext: Setup Grafana and import CSVs")
            print("="*50)
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
