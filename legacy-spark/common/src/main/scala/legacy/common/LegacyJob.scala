package legacy.common

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.streaming.StreamingQueryProgress

import scala.collection.immutable.ListMap

/** What the replay harness and the Kafka entry points need from a legacy job.
  *
  * Input of `decodeAndValidate` / `logic`: columns `arrival_seq: Long` (Kafka offset or
  * fixture sequence number) and `value: Array[Byte]` (serialized input message).
  *
  * Output of `logic` (column contract, one row per fact):
  *   - `kind`: "record" | "counter" | "state"
  *   - `vin`: String
  *   - `counter`: counter name for kind = "counter", else null
  *   - `busy`: for kind = "state" (the status marker every state-function call emits),
  *     true iff the VIN still has buffered events or an open session/window after this
  *     invocation; else null. The marker's batch is the micro-batch id the sink sees.
  *   - `record`: for kind = "record", a struct whose field names are the output proto's
  *     field names (enums as value-name strings); else null
  */
trait LegacyJob {
  def name: String
  def inputMessage: String
  def outputMessage: String
  def delayMs: Long

  /** Decoded input fields plus `arrival_seq`, `ts` and `valid: Boolean`. Shared by `logic`
    * and the harness's own late count.
    */
  def decodeAndValidate(input: DataFrame, descriptorPath: String): DataFrame

  /** The one logic function: validation, then withWatermark, then flatMapGroupsWithState. */
  def logic(input: DataFrame, descriptorPath: String): DataFrame

  /** Counters object in parity/replay/FORMAT.md's shape, from the job's counter rows
    * (name -> count) and Spark's numRowsDroppedByWatermark total.
    */
  def counters(counterCounts: Map[String, Long], late: Long): ListMap[String, Any]

  /** Flush control message for target watermark T, as fixture JSON (FORMAT.md encoding). */
  def controlMessageJson(targetWatermarkMs: Long): String
}

object LegacyJob {
  val ReservedVin = "TSTZZZZZZZZZZZZZZ"

  /** The job's `late` counter: Spark's own numRowsDroppedByWatermark, summed over every
    * trigger (no-data batches included).
    */
  def droppedByWatermark(progresses: Seq[StreamingQueryProgress]): Long =
    progresses.flatMap(_.stateOperators).map(_.numRowsDroppedByWatermark).sum

  def session(app: String): SparkSession = {
    val spark = SparkSession
      .builder()
      .master("local[1]")
      .appName(app)
      .config("spark.sql.shuffle.partitions", "1")
      .config("spark.sql.session.timeZone", "UTC")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.streaming.noDataMicroBatches.enabled", "true")
      .config("spark.sql.streaming.noDataProgressEventInterval", "86400000")
      .config("spark.sql.streaming.numRecentProgressUpdates", "100000")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    spark
  }
}
