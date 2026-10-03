package legacy.battery

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}
import java.security.MessageDigest
import java.sql.Timestamp

import com.fasterxml.jackson.databind.ObjectMapper
import legacy.common.{LegacyJob => Job}
import org.apache.spark.sql.{DataFrame, Encoder, Encoders}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{GroupState, GroupStateTimeout, OutputMode, StreamingQueryListener}
import org.apache.spark.sql.protobuf.functions.{from_protobuf, to_protobuf}

import scala.collection.immutable.ListMap
import scala.collection.mutable

object BatteryHealth extends Job {
  override val name = "battery-health"
  override val inputMessage = "vehicle.battery.v1.BatteryReading"
  override val outputMessage = "battery.health.v1.BatteryWindow"
  override val outputTopic = "battery.health.v1"
  override val delayMs = 120000L

  private val minTs = 1577836800000L
  private val maxTs = 4102444800000L
  private val windowMs = 300000L
  private val eventIds = "^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$"
  private val vins = "^[A-Z0-9]{17}$"

  case class BatteryInput(
      event_id: String,
      vin: String,
      ts: Long,
      soc_pct: Option[Double],
      soh_pct: Option[Double],
      cell_temp_max_c: Option[Double],
      pack_voltage_v: Option[Double],
      arrival_seq: Long,
      valid: Boolean,
      event_time: Timestamp)

  case class BatteryRecord(
      output_id: String,
      vin: String,
      window_start: Long,
      avg_soc_pct: Double,
      min_soh_pct: Double,
      max_cell_temp_c: Double,
      alert: Boolean,
      event_count: Long)

  case class JobOutput(
      kind: String,
      vin: String,
      counter: Option[String],
      busy: Option[Boolean],
      record: Option[BatteryRecord])

  case class Payload(
      event_id: String,
      vin: String,
      ts: Long,
      soc_pct: Option[Double],
      soh_pct: Option[Double],
      cell_temp_max_c: Option[Double],
      pack_voltage_v: Option[Double])

  case class Window(
      soc_sum: Double,
      count: Long,
      min_soh: Double,
      max_cell_temp: Double)

  case class State(
      seen: Map[String, Payload],
      buffered: Vector[BatteryInput],
      windows: Map[Long, Window])

  override def decodeAndValidate(input: DataFrame, descriptorPath: String): DataFrame = {
    val raw = input.select(
      col("arrival_seq"),
      from_protobuf(col("value"), inputMessage, descriptorPath).as("message"))
    val decoded = raw.select(
      coalesce(col("message.event_id"), lit("")).as("event_id"),
      coalesce(col("message.vin"), lit("")).as("vin"),
      coalesce(col("message.ts"), lit(0L)).as("ts"),
      col("message.soc_pct").as("soc_pct"),
      col("message.soh_pct").as("soh_pct"),
      col("message.cell_temp_max_c").as("cell_temp_max_c"),
      col("message.pack_voltage_v").as("pack_voltage_v"),
      col("arrival_seq"))
    val soc = col("soc_pct")
    val soh = col("soh_pct")
    val temp = col("cell_temp_max_c")
    val voltage = col("pack_voltage_v")
    val valid = col("event_id").rlike(eventIds) &&
      col("vin").rlike(vins) &&
      col("ts").between(minTs, maxTs - 1L) &&
      soc.isNotNull && !isnan(soc) && soc.between(0.0, 100.0) &&
      soh.isNotNull && !isnan(soh) && soh.between(0.0, 100.0) &&
      temp.isNotNull && !isnan(temp) && temp.between(-60.0, 120.0) &&
      voltage.isNotNull && !isnan(voltage) && voltage > 0.0 && voltage <= 1000.0
    decoded
      .withColumn("valid", coalesce(valid, lit(false)))
      .withColumn("event_time", timestamp_millis(col("ts")))
  }

  override def logic(input: DataFrame, descriptorPath: String): DataFrame = {
    val decoded = decodeAndValidate(input, descriptorPath)
    val rejected = decoded
      .filter(!col("valid"))
      .as[BatteryInput](Encoders.product[BatteryInput])
      .map(row => JobOutput("counter", row.vin, Some("rejected"), None, None))(
        Encoders.product[JobOutput])

    val valid = decoded
      .filter(col("valid"))
      .withColumn("event_time", timestamp_millis(col("ts")))
      .withWatermark("event_time", "2 minutes")
      .as[BatteryInput](Encoders.product[BatteryInput])

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
      rows: Iterator[BatteryInput],
      groupState: GroupState[State]): Iterator[JobOutput] = {
    val watermark = groupState.getCurrentWatermarkMs()
    val stored = groupState.getOption.getOrElse(State(Map.empty, Vector.empty, Map.empty))
    var seen = stored.seen
    var buffered = stored.buffered
    var windows = stored.windows
    val output = mutable.ArrayBuffer.empty[JobOutput]

    rows.toVector.sortBy(_.arrival_seq).foreach { row =>
      val decoded = Payload(
        row.event_id,
        row.vin,
        row.ts,
        row.soc_pct,
        row.soh_pct,
        row.cell_temp_max_c,
        row.pack_voltage_v)
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
      val start = row.ts - row.ts % windowMs
      val soc = row.soc_pct.get
      val soh = row.soh_pct.get
      val temp = row.cell_temp_max_c.get
      val window = windows.get(start) match {
        case Some(previous) =>
          Window(previous.soc_sum + soc, previous.count + 1L,
            math.min(previous.min_soh, soh), math.max(previous.max_cell_temp, temp))
        case None => Window(soc, 1L, soh, temp)
      }
      windows += start -> window
    }

    windows.toSeq.filter { case (start, _) => start + windowMs <= watermark }
      .sortBy(_._1)
      .foreach { case (start, window) =>
        val idText = s"battery-health|$vin|$start"
        val record = BatteryRecord(
          sha256(idText),
          vin,
          start,
          window.soc_sum / window.count,
          window.min_soh,
          window.max_cell_temp,
          window.max_cell_temp > 55.0,
          window.count)
        if (vin != Job.ReservedVin) output += JobOutput("record", vin, None, None, Some(record))
        windows -= start
      }

    val updated = State(seen, buffered, windows)
    groupState.update(updated)
    val timeouts = buffered.map(_.ts - 1L) ++ windows.keys.map(_ + windowMs - 1L)
    if (timeouts.nonEmpty) groupState.setTimeoutTimestamp(timeouts.min)
    output += JobOutput("state", vin, None, Some(buffered.nonEmpty || windows.nonEmpty), None)
    output.iterator
  }

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
      "conflicting_duplicates" -> counterCounts.getOrElse("conflicting_duplicates", 0L))

  override def controlMessageJson(targetWatermarkMs: Long): String =
    s"""{"event_id":"ffffffff-ffff-4fff-bfff-ffffffffffff","vin":"${Job.ReservedVin}","ts":${targetWatermarkMs + delayMs},"soc_pct":50.0,"soh_pct":100.0,"cell_temp_max_c":20.0,"pack_voltage_v":400.0}"""

  private def arguments(args: Array[String]): Map[String, String] = {
    require(args.length % 2 == 0, "arguments must be --flag value pairs")
    val defaults = Map(
      "--brokers" -> "localhost:9092",
      "--input-topic" -> "vehicle.battery.v1",
      "--output-topic" -> outputTopic,
      "--checkpoint" -> "/tmp/battery-health-checkpoint",
      "--descriptor" -> "/app/telemetry.desc",
      "--counters-out" -> "/tmp/battery-health.counters.json")
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
