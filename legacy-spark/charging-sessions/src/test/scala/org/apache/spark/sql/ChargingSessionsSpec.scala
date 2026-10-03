package org.apache.spark.sql

import java.io.ByteArrayOutputStream

import com.google.protobuf.CodedOutputStream
import legacy.charging.ChargingSessions
import legacy.common.LegacyJob
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.streaming.StreamingQuery
import org.scalatest.funsuite.AnyFunSuite

class ChargingSessionsSpec extends AnyFunSuite {
  private val descriptor = "/app/telemetry.desc"
  private val vin = "TST00000000000001"

  private def charging(
      eventId: String,
      ts: Long,
      event: Option[Int],
      energy: Option[Long],
      lat: Option[Double] = None,
      lon: Option[Double] = None,
      chargerType: Option[String] = None,
      vehicleVin: String = vin): Array[Byte] = {
    val bytes = new ByteArrayOutputStream()
    val output = CodedOutputStream.newInstance(bytes)
    output.writeString(1, eventId)
    output.writeString(2, vehicleVin)
    output.writeInt64(3, ts)
    event.foreach(output.writeEnum(4, _))
    energy.foreach(output.writeInt64(5, _))
    lat.foreach(output.writeDouble(6, _))
    lon.foreach(output.writeDouble(7, _))
    chargerType.foreach(output.writeString(8, _))
    output.flush()
    bytes.toByteArray
  }

  private def withSpark(test: SparkSession => Unit): Unit = {
    val spark = LegacyJob.session("charging-sessions-tests")
    try test(spark)
    finally spark.stop()
  }

  test("protobuf decode preserves optional presence and coalesces proto defaults") {
    withSpark { spark =>
      import spark.implicits._
      val input = Seq(
        (1L, charging("00000000-0000-4000-8000-000000000001", 1789984800000L,
          Some(1), Some(0L), Some(0.0))),
        (2L, charging("00000000-0000-4000-8000-000000000002", 1789984800000L,
          None, None))).toDF("arrival_seq", "value")
      val rows = ChargingSessions.decodeAndValidate(input, descriptor)
        .orderBy("arrival_seq")
        .collect()
      assert(!rows(0).isNullAt(rows(0).fieldIndex("lat")))
      assert(rows(0).getAs[Double]("lat") == 0.0)
      assert(rows(0).getAs[Long]("energy_wh") == 0L)
      assert(rows(1).isNullAt(rows(1).fieldIndex("lat")))
      assert(rows(1).getAs[String]("event") == "EVENT_UNSPECIFIED")
      assert(rows(1).getAs[Long]("energy_wh") == 0L)
    }
  }

  test("example 1 emits the expected session after the control and empty flush batches") {
    withSpark { spark =>
      implicit val inputEncoder: Encoder[(Long, Array[Byte])] =
        Encoders.tuple(Encoders.scalaLong, Encoders.BINARY)
      val input = MemoryStream[(Long, Array[Byte])](inputEncoder, spark.sqlContext)
      val queryName = "charging_example_1"
      val output = ChargingSessions.logic(
        input.toDF().toDF("arrival_seq", "value"), descriptor)
      var query: StreamingQuery = null
      try {
        query = output.writeStream
          .format("memory")
          .queryName(queryName)
          .outputMode("append")
          .start()
        input.addData(Seq(
          (1L, charging("00000000-0000-4000-8000-000000000101", 1789984800000L,
            Some(1), Some(0L), Some(37.4), Some(-122.1), Some("dc_fast"))),
          (2L, charging("00000000-0000-4000-8000-000000000102", 1789985400000L,
            Some(3), Some(2000L))),
          (3L, charging("00000000-0000-4000-8000-000000000103", 1789986000000L,
            Some(4), Some(500L))),
          (4L, charging("00000000-0000-4000-8000-000000000104", 1789986300000L,
            Some(5), Some(0L)))))
        query.processAllAvailable()
        val target = 1789989900000L
        input.addData(Seq((5L, charging(
          "ffffffff-ffff-4fff-bfff-ffffffffffff",
          target + ChargingSessions.delayMs,
          Some(1),
          Some(0L),
          Some(0.0),
          Some(0.0),
          Some("flush"),
          LegacyJob.ReservedVin))))
        query.processAllAvailable()
        input.addData(Seq.empty[(Long, Array[Byte])])
        query.processAllAvailable()

        val records = spark.table(queryName).filter("kind = 'record'").collect()
        assert(records.length == 1)
        val record = records.head.getAs[Row]("record")
        assert(record.getAs[String]("output_id") ==
          "16fcf70839634f753e124cbc83e9234b8e37cee619b2b9de4809e9ff3add3a2b")
        assert(record.getAs[Long]("start_ts") == 1789984800000L)
        assert(record.getAs[Long]("end_ts") == 1789986300000L)
        assert(record.getAs[Long]("duration_ms") == 1500000L)
        assert(record.getAs[Long]("total_energy_wh") == 2500L)
        assert(record.getAs[String]("close_reason") == "UNPLUG")
      } finally {
        if (query != null) query.stop()
      }
    }
  }
}
