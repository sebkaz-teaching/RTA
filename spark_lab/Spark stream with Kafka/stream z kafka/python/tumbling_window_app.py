# spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.1 tumbling_window_app.py
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

    json_schema = StructType(
        [
            StructField("time", TimestampType()),
            StructField("id", StringType()),
            StructField("value", IntegerType()),
        ]
    )

    parsed = raw.select(
        "timestamp", f.from_json(raw.value.cast("string"), json_schema).alias("json")
    ).select(
        f.col("timestamp").alias("proc_time"),
        f.col("json").getField("time").alias("event_time"),
        f.col("json").getField("id").alias("id"),
        f.col("json").getField("value").alias("value"),
    )

    # TODO: filter out from stream records with id "a" or "b"
    #  implement 10 seconds wide tumbling window, add id column to grouping
    #grouped = parsed
    
    grouped = parsed.where("id != 'a' and id !='b'")\
    .groupBy(f.window("event_time", "10 seconds", "10 seconds"),"id")\
    .count()

    # optional: change output mode to "update"
    query = (
        grouped.writeStream.outputMode("complete")
        .format("console")
        .option("truncate", "false")
        .start()
    )
    query.awaitTermination(60)
    query.stop()
