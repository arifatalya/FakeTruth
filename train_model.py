#!/usr/bin/env python3
"""
Fake News Detection Model Training - Naive Bayes
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, concat, lower, regexp_replace, length
from pyspark.ml.feature import Tokenizer, StopWordsRemover, HashingTF, IDF
from pyspark.ml.classification import NaiveBayes
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator


def create_spark_session():
    return SparkSession.builder \
        .appName("FakeNewsModelTraining") \
        .config("spark.driver.memory", "2g") \
        .getOrCreate()


def load_data(spark):
    fake_path = "file:///home/payulnaki/Proyek Akhir/Fake and News Dataset/Fake.csv"
    true_path = "file:///home/payulnaki/Proyek Akhir/Fake and News Dataset/True.csv"
    
    print("Loading datasets...")
    
    # Load CSV
    fake_df = spark.read \
        .option("header", "true") \
        .option("multiLine", "true") \
        .option("escape", '"') \
        .csv(fake_path)
    
    true_df = spark.read \
        .option("header", "true") \
        .option("multiLine", "true") \
        .option("escape", '"') \
        .csv(true_path)
    
    # Add label: 0 = Fake, 1 = True
    fake_df = fake_df.withColumn("label", lit(0.0))
    true_df = true_df.withColumn("label", lit(1.0))
    
    print(f"✓ Fake news: {fake_df.count()} articles")
    print(f"✓ True news: {true_df.count()} articles")
    
    # Combine
    df = fake_df.union(true_df)
    
    return df


def preprocess_data(df):
    """Clean and preprocess text data"""
    
    print("\nPreprocessing data...")
    
    # Combine title + text
    df = df.withColumn("content", concat(col("title"), lit(" "), col("text")))
    
    # Clean text: lowercase, remove special chars
    df = df.withColumn("content", lower(col("content")))
    df = df.withColumn("content", regexp_replace(col("content"), r"[^a-zA-Z\s]", ""))
    df = df.withColumn("content", regexp_replace(col("content"), r"\s+", " "))
    
    # Filter out empty or very short content
    df = df.filter(length(col("content")) > 50)
    
    # Select relevant columns
    df = df.select("content", "label", "subject")
    
    print(f"✓ Total samples after cleaning: {df.count()}")
    
    return df


def build_pipeline():
    """Build ML pipeline: Tokenizer → StopWords → TF-IDF → Naive Bayes"""
    
    # Tokenize text
    tokenizer = Tokenizer(inputCol="content", outputCol="words")
    
    # Remove stop words
    remover = StopWordsRemover(inputCol="words", outputCol="filtered_words")
    
    # TF (Term Frequency) - reduced features to prevent overfitting
    hashing_tf = HashingTF(inputCol="filtered_words", outputCol="raw_features", numFeatures=5000)
    
    # IDF (Inverse Document Frequency)
    idf = IDF(inputCol="raw_features", outputCol="features", minDocFreq=5)
    
    # Naive Bayes classifier
    nb = NaiveBayes(
        featuresCol="features",
        labelCol="label",
        smoothing=1.0,  # Laplace smoothing
        modelType="multinomial"
    )
    
    # Build pipeline
    pipeline = Pipeline(stages=[tokenizer, remover, hashing_tf, idf, nb])
    
    return pipeline


def train_and_evaluate(df, pipeline):
    """Train model and evaluate performance"""
    
    print("\nSplitting data (80% train, 20% test)...")
    train_df, test_df = df.randomSplit([0.8, 0.2], seed=42)
    
    print(f"✓ Training set: {train_df.count()} samples")
    print(f"✓ Test set: {test_df.count()} samples")
    
    print("\nTraining Naive Bayes model... (this may take a few minutes)")
    model = pipeline.fit(train_df)
    
    print("✓ Model trained!")
    
    # Predictions on test set
    print("\nEvaluating model...")
    predictions = model.transform(test_df)
    
    # Metrics
    binary_evaluator = BinaryClassificationEvaluator(labelCol="label", metricName="areaUnderROC")
    auc = binary_evaluator.evaluate(predictions)
    
    multi_evaluator = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction")
    
    accuracy = multi_evaluator.evaluate(predictions, {multi_evaluator.metricName: "accuracy"})
    precision = multi_evaluator.evaluate(predictions, {multi_evaluator.metricName: "weightedPrecision"})
    recall = multi_evaluator.evaluate(predictions, {multi_evaluator.metricName: "weightedRecall"})
    f1 = multi_evaluator.evaluate(predictions, {multi_evaluator.metricName: "f1"})
    
    print("\n" + "="*50)
    print("MODEL EVALUATION RESULTS (Naive Bayes)")
    print("="*50)
    print(f"Accuracy:  {accuracy:.4f} ({accuracy*100:.2f}%)")
    print(f"Precision: {precision:.4f}")
    print(f"Recall:    {recall:.4f}")
    print(f"F1 Score:  {f1:.4f}")
    print(f"AUC-ROC:   {auc:.4f}")
    print("="*50)
    
    # Show sample predictions
    print("\nSample Predictions:")
    predictions.select("content", "label", "prediction", "probability") \
        .show(10, truncate=50)
    
    return model, predictions


def save_model(model, path):
    """Save model"""
    print(f"\nSaving model to {path}...")
    model.write().overwrite().save(path)
    print("✓ Model saved!")


def main():
    print("="*50)
    print("FAKE NEWS DETECTION - NAIVE BAYES")
    print("="*50)
    
    # Create Spark session
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    
    try:
        # Load data
        df = load_data(spark)
        
        # Show sample
        print("\nSample data:")
        df.select("title", "subject", "label").show(5, truncate=40)
        
        # Preprocess
        df = preprocess_data(df)
        
        # Build pipeline
        pipeline = build_pipeline()
        
        # Train and evaluate
        model, predictions = train_and_evaluate(df, pipeline)
        
        # Save model to HDFS
        hdfs_model_path = "hdfs://localhost:9000/user/news/model/fake_news_classifier"
        save_model(model, hdfs_model_path)
        
        # Also save locally as backup
        local_model_path = "/home/payulnaki/fake_news_model"
        save_model(model, local_model_path)
        
        print("\n" + "="*50)
        print("✅ TRAINING COMPLETE!")
        print("="*50)
        print(f"Model: Naive Bayes")
        print(f"Saved to:")
        print(f"  - HDFS: {hdfs_model_path}")
        print(f"  - Local: {local_model_path}")
        print("\nNext: spark-submit streaming_with_prediction.py")
        print("="*50)
        
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
