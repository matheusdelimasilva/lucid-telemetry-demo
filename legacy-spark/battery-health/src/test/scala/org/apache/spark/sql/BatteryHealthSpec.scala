package org.apache.spark.sql

import java.io.ByteArrayOutputStream

import com.google.protobuf.CodedOutputStream
import legacy.battery.BatteryHealth
import legacy.common.LegacyJob
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.streaming.StreamingQuery
import org.scalatest.funsuite.AnyFunSuite

class BatteryHealthSpec extends AnyFunSuite {
  private val descriptor = "/app/telemetry.desc"
  private val vin = "TST00000000000015"

  private def reading(
      eventId: String,
      ts: Long,
      soc: Option[Double] = None,
      soh: Option[Double] = None,
      temp: Option[Double] = None,
      voltage: Option[Double] = None,
      vehicleVin: String = vin): Array[Byte] = {
    val bytes = new ByteArrayOutputStream()
    val output = CodedOutputStream.newInstance(bytes)
    output.writeString(1, eventId)
    output.writeString(2, vehicleVin)
    output.writeInt64(3, ts)
    soc.foreach(output.writeDouble(4, _))
    soh.foreach(output.writeDouble(5, _))
    temp.foreach(output.writeDouble(6, _))
    voltage.foreach(output.writeDouble(7, _))
    output.flush()
    bytes.toByteArray
  }

  private def withSpark(test: SparkSession => Unit): Unit = {
    val spark = LegacyJob.session("battery-health-tests")
    try test(spark)
    finally spark.stop()
  }

  test("protobuf decode preserves optional presence and explicit zero values") {
    withSpark { spark =>
      import spark.implicits._
      val input = Seq(
        (1L, reading("00000000-0000-4000-8000-000000000001", 1790000100000L,
          Some(0.0), Some(100.0), Some(20.0), Some(400.0))),
        (2L, reading("00000000-0000-4000-8000-000000000002", 1790000100000L)))
        .toDF("arrival_seq", "value")
      val rows = BatteryHealth.decodeAndValidate(input, descriptor)
        .orderBy("arrival_seq")
        .collect()
      assert(!rows(0).isNullAt(rows(0).fieldIndex("soc_pct")))
      assert(rows(0).getAs[Double]("soc_pct") == 0.0)
      assert(rows(1).isNullAt(rows(1).fieldIndex("soc_pct")))
      assert(rows(0).getAs[Boolean]("valid"))
      assert(!rows(1).getAs[Boolean]("valid"))
    }
  }

  test("example 15 emits both boundary windows after the control and empty flush batches") {
    withSpark { spark =>
      implicit val inputEncoder: Encoder[(Long, Array[Byte])] =
        Encoders.tuple(Encoders.scalaLong, Encoders.BINARY)
      val input = MemoryStream[(Long, Array[Byte])](inputEncoder, spark.sqlContext)
      val queryName = "battery_example_15"
      val output = BatteryHealth.logic(
        input.toDF().toDF("arrival_seq", "value"), descriptor)
      var query: StreamingQuery = null
      try {
        query = output.writeStream
          .format("memory")
          .queryName(queryName)
          .outputMode("append")
          .start()
        input.addData(Seq(
          (1L, reading("00000000-0000-4000-8000-000000001501", 1790000099999L,
            Some(80.0), Some(95.0), Some(30.0), Some(400.0))),
          (2L, reading("00000000-0000-4000-8000-000000001502", 1790000100000L,
            Some(80.0), Some(95.0), Some(30.0), Some(400.0)))))
        query.processAllAvailable()
        val target = 1790003700000L
        input.addData(Seq((3L, reading(
          "ffffffff-ffff-4fff-bfff-ffffffffffff",
          target + BatteryHealth.delayMs,
          Some(50.0),
          Some(100.0),
          Some(20.0),
          Some(400.0),
          LegacyJob.ReservedVin))))
        query.processAllAvailable()
        input.addData(Seq.empty[(Long, Array[Byte])])
        query.processAllAvailable()

        val records = spark.table(queryName).filter("kind = 'record'").collect()
          .map(_.getAs[Row]("record"))
          .sortBy(_.getAs[Long]("window_start"))
        assert(records.length == 2)
        assert(records(0).getAs[String]("output_id") ==
          "cbb8823f0ae1dcedaa8c3d5eb88fbf1c4100d1b859faf5aa567c4027983f5499")
        assert(records(1).getAs[String]("output_id") ==
          "aebd17c95a2ece7b3dbce356edcf3f012610d54644a25e2b92ee01f02150930a")
        assert(records.map(_.getAs[Long]("event_count")).toSeq == Seq(1L, 1L))
        assert(records.forall(_.getAs[Double]("avg_soc_pct") == 80.0))
      } finally {
        if (query != null) query.stop()
      }
    }
  }
}
