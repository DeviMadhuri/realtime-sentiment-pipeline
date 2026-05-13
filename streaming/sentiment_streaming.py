"""
Spark Structured Streaming job.

Reads reviews from Kafka, runs sentiment analysis with a HuggingFace
transformer through a pandas UDF (so the model loads once per executor
and inference is batched), then writes the labeled output to a Delta
table on local disk.
"""

import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, pandas_udf
from pyspark.sql.types import StringType, StructField, StructType


# Lazy-loaded HuggingFace pipeline so it doesn't try to download the
# model at driver startup. Each executor builds its own copy on first call.
_pipeline = None


def get_pipeline():
    global _pipeline
    if _pipeline is None:
        from transformers import pipeline

        _pipeline = pipeline(
            "sentiment-analysis",
            model="distilbert-base-uncased-finetuned-sst-2-english",
        )
    return _pipeline


@pandas_udf("struct<label:string, score:float>")
def analyze_sentiment(texts: pd.Series) -> pd.DataFrame:
    pipe = get_pipeline()
    # The pipeline accepts a list and returns a list of dicts with
    # 'label' and 'score'. Convert to a DataFrame so Spark can map it
    # back to the struct schema we declared above.
    results = pipe(texts.tolist())
    return pd.DataFrame(results)


def build_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("RealtimeSentimentPipeline")
        .config(
            "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1,"
            "io.delta:delta-core_2.12:2.4.0",
        )
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .getOrCreate()
    )


def main() -> None:
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    review_schema = StructType([
        StructField("review_id", StringType()),
        StructField("product", StringType()),
        StructField("review_text", StringType()),
        StructField("timestamp", StringType()),
    ])

    # Read the raw Kafka stream
    raw_stream = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "localhost:9092")
        .option("subscribe", "reviews")
        .option("startingOffsets", "latest")
        .load()
    )

    # Parse JSON payloads, run sentiment, flatten the struct
    parsed = (
        raw_stream
        .select(from_json(col("value").cast("string"), review_schema).alias("data"))
        .select("data.*")
        .withColumn("sentiment", analyze_sentiment(col("review_text")))
        .select(
            "review_id",
            "product",
            "review_text",
            "timestamp",
            col("sentiment.label").alias("sentiment_label"),
            col("sentiment.score").alias("sentiment_score"),
        )
    )

    # Write to Delta in append mode
    query = (
        parsed.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", "./data/checkpoints/reviews")
        .start("./data/delta/reviews_sentiment")
    )

    print("Streaming started. Writing labeled reviews to ./data/delta/reviews_sentiment")
    print("Press Ctrl+C to stop.\n")
    query.awaitTermination()


if __name__ == "__main__":
    main()
