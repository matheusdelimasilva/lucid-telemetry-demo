import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.protobuf.functions.from_protobuf
import smoke.smoke.Smoke

// Stage-1 throwaway: proves ScalaPB codegen + from_protobuf decoding under the
// pinned Spark image. Deleted with the rest of spark-hello after stage 1.
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
    val msg = Smoke(
      vin = "TST00000000000003",
      ts = 1790000100000L,
      note = "stage1 smoke from spark"
    )
    ms.addData(msg.toByteArray)

    val decoded = ms
      .toDF()
      .select(from_protobuf(col("value"), "smoke.Smoke", "/app/smoke.desc").as("m"))
      .select("m.*")

    val query = decoded.writeStream.format("memory").queryName("out").start()
    query.processAllAvailable()

    spark.table("out").show(false)
    val row = spark.table("out").first()
    println(
      s"DECODED vin=${row.getString(0)} ts=${row.getLong(1)} note=${row.getString(2)}"
    )

    query.stop()
    spark.stop()
  }
}
