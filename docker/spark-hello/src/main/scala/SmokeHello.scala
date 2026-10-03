import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.protobuf.functions.from_protobuf
import vehicle.charging.v1.vehicle_charging_v1.{ChargingEvent, ChargingEventType}

// Stage-2 smoke check: proves ScalaPB codegen + from_protobuf decoding of a
// real telemetry message under the pinned Spark image. No job logic.
object SmokeHello {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[1]")
      .appName("smoke-hello")
      .config("spark.sql.shuffle.partitions", "1")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    import spark.implicits._
    implicit val sqlCtx = spark.sqlContext

    val ms = MemoryStream[Array[Byte]]
    val msg = ChargingEvent(
      eventId = "00000000-0000-4000-8000-000000000001",
      vin = "TST00000000000003",
      ts = 1790000100000L,
      event = ChargingEventType.PLUG_IN,
      energyWh = 0L,
      lat = Some(37.4),
      lon = Some(-122.1),
      chargerType = Some("dc_fast")
    )
    ms.addData(msg.toByteArray)

    val decoded = ms
      .toDF()
      .select(
        from_protobuf(
          col("value"),
          "vehicle.charging.v1.ChargingEvent",
          "/app/telemetry.desc"
        ).as("m")
      )
      .select("m.*")

    val query = decoded.writeStream.format("memory").queryName("out").start()
    query.processAllAvailable()

    spark.table("out").show(false)
    val row = spark.table("out").first()
    println(
      s"DECODED vin=${row.getAs[String]("vin")} ts=${row.getAs[Long]("ts")} " +
        s"event=${row.getAs[String]("event")} lat=${row.getAs[Double]("lat")}"
    )

    query.stop()
    spark.stop()
  }
}
