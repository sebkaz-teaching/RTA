# spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.1 raw_app.py
import sys

from pyspark.sql import SparkSession


if __name__ == "__main__":
    server = sys.argv[1] if len(sys.argv) == 2 else "localhost:9092"

    spark = SparkSession.builder.appName("stream").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    # topic subscription
    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", server)
        .option("subscribe", "topicX")
        .load()
    )

    # defining output
    query = raw.writeStream.outputMode("append").format("console").start()
    query.awaitTermination(60)
    query.stop()
