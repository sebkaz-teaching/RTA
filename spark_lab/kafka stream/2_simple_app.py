# spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.1 2_simple_app.py
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    TimestampType,
)


if __name__ == "__main__":
    server = sys.argv[1] if len(sys.argv) == 2 else "localhost:9092"

    spark = SparkSession.builder.appName("stream").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", server)
        .option("subscribe", "topicX")
        .load()
    )

    # json message schema
    json_schema = StructType(
        [
            StructField("time", TimestampType()),
            StructField("id", StringType()),
            StructField("value", IntegerType()),
        ]
    )

    # transforming incoming data
    parsed = raw.select(
        "timestamp", f.from_json(raw.value.cast("string"), json_schema).alias("json")
    ).select(
        f.col("timestamp").alias("proc_time"),
        f.col("json").getField("time").alias("event_time"),
        f.col("json").getField("id").alias("id"),
        f.col("json").getField("value").alias("value"),
    )

    query = (
        parsed.writeStream.outputMode("append")
        .format("console")
        .option("truncate", "false")
        .start()
    )
    query.awaitTermination(60)
    query.stop()

# query = (
#    parsed.writeStream.outputMode("append")
#    .format("kafka")
#    .option("kafka.bootstrap.servers", "host:9092")
#    .option("topic", "topic_name")
#    .option("checkpointLocation", "chckpt")
#    .start()
# )
