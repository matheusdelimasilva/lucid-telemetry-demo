package legacy.charging

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}
import java.security.MessageDigest
import java.sql.Timestamp

import com.fasterxml.jackson.databind.ObjectMapper
import legacy.common.{LegacyJob => Job}
import org.apache.spark.sql.{DataFrame, Dataset, Encoder, Encoders, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{GroupState, GroupStateTimeout, OutputMode, StreamingQueryListener}
import org.apache.spark.sql.protobuf.functions.{from_protobuf, to_protobuf}

import scala.collection.immutable.ListMap
import scala.collection.mutable

object ChargingSessions extends Job {
  override val name = "charging-sessions"
  override val inputMessage = "vehicle.charging.v1.ChargingEvent"
  override val outputMessage = "charging.sessions.v1.ChargingSession"
  override val outputTopic = "charging.sessions.v1"
  override val delayMs = 600000L

  private val minTs = 1577836800000L
  private val maxTs = 4102444800000L
  private val inactivityMs = 30L * 60L * 1000L
  private val eventIds = "^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$"
  private val vins = "^[A-Z0-9]{17}$"

  case class ChargingInput(
      event_id: String,
      vin: String,
      ts: Long,
      event: String,
      energy_wh: Long,
      lat: Option[Double],
      lon: Option[Double],
      charger_type: Option[String],
      arrival_seq: Long,
      valid: Boolean,
      event_time: Timestamp)

  case class ChargingRecord(
      output_id: String,
      vin: String,
      start_ts: Long,
      end_ts: Long,
      duration_ms: Long,
      total_energy_wh: Long,
      start_lat: Double,
      start_lon: Double,
      charger_type: String,
      close_reason: String)

  case class JobOutput(
      kind: String,
      vin: String,
      counter: Option[String],
      busy: Option[Boolean],
      record: Option[ChargingRecord])

  case class Payload(
      event_id: String,
      vin: String,
      ts: Long,
      event: String,
      energy_wh: Long,
      lat: Option[Double],
      lon: Option[Double],
      charger_type: Option[String])

  case class Session(
      output_id: String,
      start_ts: Long,
      last_ts: Long,
      total_energy_wh: Long,
      start_lat: Double,
      start_lon: Double,
      charger_type: String)

  case class State(
      seen: Map[String, Payload],
      buffered: Vector[ChargingInput],
      session: Option[Session])

  override def decodeAndValidate(input: DataFrame, descriptorPath: String): DataFrame = {
    val raw = input.select(
      col("arrival_seq"),
      from_protobuf(col("value"), inputMessage, descriptorPath).as("message"))
    val decoded = raw.select(
      coalesce(col("message.event_id"), lit("")).as("event_id"),
      coalesce(col("message.vin"), lit("")).as("vin"),
      coalesce(col("message.ts"), lit(0L)).as("ts"),
      coalesce(col("message.event"), lit("EVENT_UNSPECIFIED")).as("event"),
      coalesce(col("message.energy_wh"), lit(0L)).as("energy_wh"),
      col("message.lat").as("lat"),
      col("message.lon").as("lon"),
      col("message.charger_type").as("charger_type"),
      col("arrival_seq"))
    val plugIn = col("event") === "PLUG_IN"
    val eventValid = col("event").isin("PLUG_IN", "START", "PROGRESS", "STOP", "UNPLUG")
    val energyValid = col("energy_wh") >= 0 &&
      (col("event").isin("PROGRESS", "STOP") || col("energy_wh") === 0L)
    val plugInValid = col("lat").isNotNull && !isnan(col("lat")) &&
      col("lat").between(-90.0, 90.0) &&
      col("lon").isNotNull && !isnan(col("lon")) &&
      col("lon").between(-180.0, 180.0) &&
      col("charger_type").isNotNull && length(col("charger_type")) > 0
    val valid = col("event_id").rlike(eventIds) &&
      col("vin").rlike(vins) &&
      col("ts").between(minTs, maxTs - 1L) &&
      eventValid && energyValid && (!plugIn || plugInValid)
    decoded
      .withColumn("valid", coalesce(valid, lit(false)))
      .withColumn("event_time", timestamp_millis(col("ts")))
  }

  override def logic(input: DataFrame, descriptorPath: String): DataFrame = {
    val decoded = decodeAndValidate(input, descriptorPath)
    val rejected = decoded
      .filter(!col("valid"))
      .as[ChargingInput](Encoders.product[ChargingInput])
      .map(row => JobOutput("counter", row.vin, Some("rejected"), None, None))(
        Encoders.product[JobOutput])

    val valid = decoded
      .filter(col("valid"))
      .withColumn("event_time", timestamp_millis(col("ts")))
      .withWatermark("event_time", "10 minutes")
      .as[ChargingInput](Encoders.product[ChargingInput])

    val stateful = valid
      .groupByKey(_.vin)(Encoders.STRING)
      .flatMapGroupsWithState[State, JobOutput](
        OutputMode.Append(),
        GroupStateTimeout.EventTimeTimeout())(updateState)(
        Encoders.kryo[State],
        Encoders.product[JobOutput])
    stateful.toDF().unionByName(rejected.toDF())
  }

  private def updateState(
      vin: String,
      rows: Iterator[ChargingInput],
      groupState: GroupState[State]): Iterator[JobOutput] = {
    val watermark = groupState.getCurrentWatermarkMs()
    val stored = groupState.getOption.getOrElse(State(Map.empty, Vector.empty, None))
    var seen = stored.seen
    var buffered = stored.buffered
    var session = stored.session
    val output = mutable.ArrayBuffer.empty[JobOutput]

    rows.toVector.sortBy(_.arrival_seq).foreach { row =>
      val decoded = Payload(row.event_id, row.vin, row.ts, row.event, row.energy_wh,
        row.lat, row.lon, row.charger_type)
      seen.get(row.event_id) match {
        case Some(previous) =>
          if (vin != Job.ReservedVin) {
            val counter = if (previous == decoded) "duplicate_events" else "conflicting_duplicates"
            output += JobOutput("counter", vin, Some(counter), None, None)
          }
        case None =>
          seen += row.event_id -> decoded
          buffered :+= row
      }
    }

    val ready = buffered.filter(_.ts <= watermark).sortBy(row => (row.ts, row.arrival_seq))
    buffered = buffered.filter(_.ts > watermark)
    ready.foreach { row =>
      if (session.exists(open => row.ts - open.last_ts > inactivityMs)) {
        val open = session.get
        output ++= close(vin, open, open.last_ts, "INACTIVITY_TIMEOUT")
        session = None
      }
      row.event match {
        case "PLUG_IN" =>
          session.foreach { open =>
            output ++= close(vin, open, open.last_ts, "REPLACED_BY_PLUG_IN")
          }
          session = Some(Session(
            sha256(row.event_id),
            row.ts,
            row.ts,
            0L,
            roundCoordinate(row.lat.get),
            roundCoordinate(row.lon.get),
            row.charger_type.get))
        case _ if session.isEmpty =>
          if (vin != Job.ReservedVin) output += JobOutput("counter", vin, Some("orphan"), None, None)
        case "PROGRESS" | "STOP" =>
          val open = session.get
          session = Some(open.copy(
            last_ts = row.ts,
            total_energy_wh = open.total_energy_wh + row.energy_wh))
        case "START" =>
          session = Some(session.get.copy(last_ts = row.ts))
        case "UNPLUG" =>
          val open = session.get
          output ++= close(vin, open, row.ts, "UNPLUG")
          session = None
        case _ =>
      }
    }

    if (session.exists(open => open.last_ts + inactivityMs < watermark)) {
      val open = session.get
      output ++= close(vin, open, open.last_ts, "INACTIVITY_TIMEOUT")
      session = None
    }

    val updated = State(seen, buffered, session)
    groupState.update(updated)
    val timeouts = buffered.map(_.ts - 1L) ++ session.map(_.last_ts + inactivityMs)
    if (timeouts.nonEmpty) groupState.setTimeoutTimestamp(timeouts.min)
    output += JobOutput("state", vin, None, Some(buffered.nonEmpty || session.nonEmpty), None)
    output.iterator
  }

  private def close(vin: String, open: Session, endTs: Long, reason: String): Seq[JobOutput] = {
    if (vin == Job.ReservedVin) return Seq.empty
    val reasonCounter = reason.toLowerCase(java.util.Locale.ROOT)
    val record = ChargingRecord(
      open.output_id,
      vin,
      open.start_ts,
      endTs,
      endTs - open.start_ts,
      open.total_energy_wh,
      open.start_lat,
      open.start_lon,
      open.charger_type,
      reason)
    Seq(
      JobOutput("record", vin, None, None, Some(record)),
      JobOutput("counter", vin, Some(reasonCounter), None, None))
  }

  private def roundCoordinate(value: Double): Double =
    BigDecimal(value).setScale(3, BigDecimal.RoundingMode.HALF_UP).toDouble

  private def sha256(value: String): String =
    MessageDigest.getInstance("SHA-256")
      .digest(value.getBytes(StandardCharsets.UTF_8))
      .map(byte => f"${byte & 0xff}%02x")
      .mkString

  override def counters(counterCounts: Map[String, Long], late: Long): ListMap[String, Any] =
    ListMap(
      "rejected" -> counterCounts.getOrElse("rejected", 0L),
      "late" -> late,
      "duplicate_events" -> counterCounts.getOrElse("duplicate_events", 0L),
      "conflicting_duplicates" -> counterCounts.getOrElse("conflicting_duplicates", 0L),
      "orphan" -> counterCounts.getOrElse("orphan", 0L),
      "sessions_by_close_reason" -> ListMap(
        "unplug" -> counterCounts.getOrElse("unplug", 0L),
        "inactivity_timeout" -> counterCounts.getOrElse("inactivity_timeout", 0L),
        "replaced_by_plug_in" -> counterCounts.getOrElse("replaced_by_plug_in", 0L)))

  override def controlMessageJson(targetWatermarkMs: Long): String =
    s"""{"event_id":"ffffffff-ffff-4fff-bfff-ffffffffffff","vin":"${Job.ReservedVin}","ts":${targetWatermarkMs + delayMs},"event":"PLUG_IN","energy_wh":0,"lat":0.0,"lon":0.0,"charger_type":"flush"}"""

  private def arguments(args: Array[String]): Map[String, String] = {
    require(args.length % 2 == 0, "arguments must be --flag value pairs")
    val defaults = Map(
      "--brokers" -> "localhost:9092",
      "--input-topic" -> "vehicle.charging.v1",
      "--output-topic" -> outputTopic,
      "--checkpoint" -> "/tmp/charging-sessions-checkpoint",
      "--descriptor" -> "/app/telemetry.desc",
      "--counters-out" -> "/tmp/charging-sessions.counters.json")
    val supplied = args.grouped(2).map(pair => pair(0) -> pair(1)).toMap
    require(supplied.keys.forall(defaults.contains), "unknown command-line flag")
    defaults ++ supplied
  }

  def main(args: Array[String]): Unit = {
    val config = arguments(args)
    val spark = Job.session(name)
    val counts = mutable.Map.empty[String, Long].withDefaultValue(0L)
    var late = 0L
    val lock = new Object
    val mapper = new ObjectMapper()
    def writeCounters(): Unit = lock.synchronized {
      val json = mapper.writeValueAsBytes(counters(counts.toMap, late))
      val path = Paths.get(config("--counters-out"))
      Option(path.getParent).foreach(parent => Files.createDirectories(parent))
      Files.write(path, json)
    }
    spark.streams.addListener(new StreamingQueryListener {
      override def onQueryStarted(event: StreamingQueryListener.QueryStartedEvent): Unit = ()
      override def onQueryProgress(event: StreamingQueryListener.QueryProgressEvent): Unit = {
        lock.synchronized {
          late += event.progress.stateOperators.map(_.numRowsDroppedByWatermark).sum
          writeCounters()
        }
      }
      override def onQueryTerminated(event: StreamingQueryListener.QueryTerminatedEvent): Unit = ()
    })

    writeCounters()
    val kafka = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", config("--brokers"))
      .option("subscribe", config("--input-topic"))
      .option("startingOffsets", "earliest")
      .load()
      .select(col("offset").as("arrival_seq"), col("value"))
    val query = logic(kafka, config("--descriptor"))
      .writeStream
      .option("checkpointLocation", config("--checkpoint"))
      .foreachBatch { (batch: DataFrame, _: Long) =>
        val rows = batch.filter(col("kind") === "counter").collect()
        lock.synchronized {
          rows.foreach { row =>
            if (row.getAs[String]("vin") != Job.ReservedVin) {
              val counter = row.getAs[String]("counter")
              counts.update(counter, counts(counter) + 1L)
            }
          }
        }
        val records = batch
          .filter(col("kind") === "record")
          .select(
            col("vin").as("key"),
            to_protobuf(col("record"), outputMessage, config("--descriptor")).as("value"))
        records.write
          .format("kafka")
          .option("kafka.bootstrap.servers", config("--brokers"))
          .option("topic", config("--output-topic"))
          .save()
        writeCounters()
      }
      .start()
    query.awaitTermination()
  }
}
