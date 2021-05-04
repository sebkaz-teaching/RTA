// spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.1 target/scala-2.12/scala_2.12-0.1.jar
import java.sql.Timestamp

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming._
import org.apache.spark.sql.types.{StructType, StringType, IntegerType, TimestampType}
import org.apache.spark.sql.{functions => f}


object SessionWindow {

  def main(args: Array[String]): Unit = {
    val server = if (args.length == 1) args(0) else "localhost:9092"
    val spark = SparkSession.builder.master("local").appName("stream").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    import spark.implicits._

    val raw = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", server)
      .option("subscribe", "topicX")
      .load()

    val json_schema = new StructType()
      .add("time", TimestampType)
      .add("id", StringType)
      .add("value", IntegerType)

    val parsed = raw.select(
        $"timestamp", f.from_json($"value".cast("string"), json_schema).alias("json")
    ).select(
        $"timestamp".alias("proc_time"),
        $"json".getField("time").alias("event_time"),
        $"json".getField("id").alias("id"),
        $"json".getField("value").alias("value"),
    )

    val events = parsed.as[(Timestamp, Timestamp, String, Int)]
      .map { case (_, timestamp, id, _) => Event(id, timestamp) }



    // Sessionize the events. Track number of events, start and end timestamps of session,
    // and report session updates.
    val sessionUpdates = events
      .groupByKey(event => event.sessionId)
      .mapGroupsWithState[SessionInfo, SessionUpdate](GroupStateTimeout.ProcessingTimeTimeout) {

        case (sessionId: String, events: Iterator[Event], state: GroupState[SessionInfo]) =>

          // If timed out, then remove session and send final update
          if (state.hasTimedOut) {
            val finalUpdate =
              SessionUpdate(sessionId, state.get.durationMs, state.get.numEvents, expired = true)
            state.remove()
            finalUpdate
          } else {
            // Update start and end timestamps in session
            val timestamps = events.map(_.timestamp.getTime).toSeq
            val updatedSession = if (state.exists) {
              val oldSession = state.get
              SessionInfo(
                oldSession.numEvents + timestamps.size,
                oldSession.startTimestampMs,
                math.max(oldSession.endTimestampMs, timestamps.max))
            } else {
              SessionInfo(timestamps.size, timestamps.min, timestamps.max)
            }
            state.update(updatedSession)

            // Set timeout such that the session will be expired if no data received for 10 seconds
            state.setTimeoutDuration("10 seconds")
            SessionUpdate(sessionId, state.get.durationMs, state.get.numEvents, expired = false)
          }
      }

    // Start running the query that prints the session updates to the console
    val query = sessionUpdates
      .writeStream
      .outputMode("update")
      .format("console")
      .start()

    query.awaitTermination(60000)
    query.stop()
  }
}
/** User-defined data type representing the input events */
case class Event(sessionId: String, timestamp: Timestamp)

/**
 * User-defined data type for storing a session information as state in mapGroupsWithState.
 *
 * @param numEvents        total number of events received in the session
 * @param startTimestampMs timestamp of first event received in the session when it started
 * @param endTimestampMs   timestamp of last event received in the session before it expired
 */
case class SessionInfo(
    numEvents: Int,
    startTimestampMs: Long,
    endTimestampMs: Long) {

  /** Duration of the session, between the first and last events */
  def durationMs: Long = endTimestampMs - startTimestampMs
}

/**
 * User-defined data type representing the update information returned by mapGroupsWithState.
 *
 * @param id          Id of the session
 * @param durationMs  Duration the session was active, that is, from first event to its expiry
 * @param numEvents   Number of events received by the session while it was active
 * @param expired     Is the session active or expired
 */
case class SessionUpdate(
    id: String,
    durationMs: Long,
    numEvents: Int,
    expired: Boolean)
