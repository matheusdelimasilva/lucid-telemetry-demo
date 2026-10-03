package legacy.replay

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, Paths}
import java.security.MessageDigest
import java.time.Instant
import java.util.concurrent.atomic.AtomicReference

import scala.collection.JavaConverters._
import scala.collection.mutable

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper, SerializationFeature}
import com.fasterxml.jackson.databind.node.ObjectNode
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.google.protobuf.{DescriptorProtos, Descriptors, DynamicMessage}
import com.google.protobuf.util.JsonFormat
import legacy.battery.BatteryHealth
import legacy.charging.ChargingSessions
import legacy.common.LegacyJob
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.execution.streaming.{FlatMapGroupsWithStateExec, MemoryStream, StreamExecution, StreamingQueryWrapper}
import org.apache.spark.sql.functions.{col, to_json}
import org.apache.spark.sql.streaming.StreamingQueryProgress

case class ReplayRow(arrival_seq: Long, value: Array[Byte])

/** Spark replay harness (parity/replay/FORMAT.md).
  *
  * Usage: Harness --descriptor <telemetry.desc> --out <dir> <suite-dir>...
  * Each suite dir holds <nn>-<slug>/{input.jsonl,expected.json}. Exits 1 on any
  * mismatch, late-count disagreement or failed drain check.
  */
object Harness {
  val mapper: ObjectMapper = new ObjectMapper().registerModule(DefaultScalaModule)
  val AvgTolerance = 1e-9
  val ToleranceFields = Set("avg_soc_pct")
  val Jobs: Map[String, LegacyJob] = Seq[LegacyJob](ChargingSessions, BatteryHealth).map(j => j.name -> j).toMap

  case class FixtureRow(arrivalSeq: Long, batch: Int, messageJson: String)
  case class Fixture(rows: Seq[FixtureRow], target: Long, flushBatch: Int)
  case class Collected(kind: String, vin: String, counter: String, busy: java.lang.Boolean, recordJson: String)
  case class MicroBatch(batchId: Long, lateFilterWm: Option[Long], evictionWm: Option[Long], rows: Seq[Collected])
  case class BatchRun(batch: Int, rows: Seq[ReplayRow], progresses: Seq[StreamingQueryProgress], mbs: Seq[MicroBatch])

  def main(args: Array[String]): Unit = {
    var descriptor = "/app/telemetry.desc"
    var out = "build/spark-examples"
    val suites = mutable.ArrayBuffer[String]()
    val fixtures = mutable.ArrayBuffer[(String, String)]()
    val it = args.iterator
    while (it.hasNext) it.next() match {
      case "--descriptor" => descriptor = it.next()
      case "--out" => out = it.next()
      case "--fixture" =>
        val spec = it.next().split("=", 2)
        require(spec.length == 2 && Jobs.contains(spec(0)),
          "--fixture must be <job>=<path> with job in " + Jobs.keys.mkString(","))
        fixtures += spec(0) -> spec(1)
      case s => suites += s
    }
    require(suites.nonEmpty || fixtures.nonEmpty,
      "usage: Harness --descriptor <desc> --out <dir> [--fixture <job>=<path>]... <suite-dir>...")

    val spark = LegacyJob.session("replay-harness")
    if (spark.conf.get("spark.sql.streaming.noDataMicroBatches.enabled", "true") != "true") {
      System.err.println(
        "REFUSED: spark.sql.streaming.noDataMicroBatches.enabled must be true (STAGE3.md finding 2)")
      sys.exit(2)
    }
    val messages = loadDescriptors(descriptor)
    val outDir = Paths.get(out)
    val summary = mutable.ArrayBuffer[ObjectNode]()

    for (suite <- suites; caseDir <- caseDirs(Paths.get(suite))) {
      val caseId = s"${Paths.get(suite).getFileName}/${caseDir.getFileName}"
      val result = runCase(spark, descriptor, messages, caseDir, outDir.resolve(caseId))
      result.put("case", caseId)
      summary += result
      val status = result.get("status").asText()
      val why = result.get("failures").elements().asScala.map(_.asText()).mkString("; ")
      println(s"$status $caseId${if (why.nonEmpty) " -- " + why else ""}")
    }

    var fixtureFailed = 0
    for ((jobName, path) <- fixtures) {
      val start = System.nanoTime()
      val status = runFixture(spark, descriptor, messages, jobName, Paths.get(path), outDir)
      val elapsed = (System.nanoTime() - start) / 1e9
      println(f"$status fixture $jobName ($elapsed%.1f s)")
      if (status != "PASS") fixtureFailed += 1
    }
    spark.stop()

    val sumNode = mapper.createArrayNode()
    summary.foreach(n => sumNode.add(n))
    write(outDir.resolve("summary.json"), pretty(sumNode))
    val failed = summary.count(_.get("status").asText() != "PASS")
    println(s"${summary.size - failed}/${summary.size} cases passed")
    if (failed > 0 || fixtureFailed > 0) sys.exit(1)
  }

  def caseDirs(suite: Path): Seq[Path] =
    Files.list(suite).iterator().asScala
      .filter(p => Files.isDirectory(p) && p.getFileName.toString.matches("""\d\d-.*"""))
      .toSeq.sortBy(_.getFileName.toString)

  /** Everything one replay produces, before any expected.json comparison. Shared by
    * suite mode (which then compares and writes per-case files) and whole-job fixture
    * mode (which writes <job>.* files under --out instead).
    */
  case class ReplayOutcome(
      job: LegacyJob,
      records: Seq[JsonNode],
      counters: JsonNode,
      trace: Seq[ObjectNode],
      drain: ObjectNode,
      failures: Seq[String],
      drainFailures: Seq[String])

  def runReplay(spark: SparkSession, descriptor: String, messages: Map[String, Descriptors.Descriptor],
                job: LegacyJob, fixture: Fixture): ReplayOutcome = {
    import spark.implicits._
    implicit val sqlCtx = spark.sqlContext

    val inDesc = messages(job.inputMessage)
    val failures = mutable.ArrayBuffer[String]()

    val events = fixture.rows.map(r => ReplayRow(r.arrivalSeq, encode(inDesc, r.messageJson)))
    val batchOf = fixture.rows.map(r => r.arrivalSeq -> r.batch).toMap
    val controlSeq = fixture.rows.map(_.arrivalSeq).max + 1
    val control = ReplayRow(controlSeq, encode(inDesc, job.controlMessageJson(fixture.target)))

    // The harness's own view of validity and ts, from the job's validation (static, no watermark).
    val validRows = job.decodeAndValidate((events :+ control).toDF(), descriptor)
      .where(col("valid")).select("arrival_seq", "ts", "vin").collect()
    val validTs: Map[Long, Long] = validRows.map(r => r.getLong(0) -> r.getLong(1)).toMap
    val vinOf: Map[Long, String] = validRows.map(r => r.getLong(0) -> r.getString(2)).toMap
    val validFixtureTs = fixture.rows.flatMap(r => validTs.get(r.arrivalSeq))
    if (validFixtureTs.isEmpty || validFixtureTs.max + 3600000L != fixture.target)
      failures += s"flush target ${fixture.target} != largest valid ts + 1 h"

    val checkpoint = Files.createTempDirectory("replay-cp-")
    val stream = MemoryStream[ReplayRow]
    val captured = mutable.ArrayBuffer[MicroBatch]()
    val execRef = new AtomicReference[StreamExecution]()
    val sink: (DataFrame, Long) => Unit = (df, batchId) => {
      val fm = execRef.get().lastExecution.executedPlan.collect { case f: FlatMapGroupsWithStateExec => f }.headOption
      val rows = df.select(col("kind"), col("vin"), col("counter"), col("busy"), to_json(col("record")))
        .collect().toSeq.map(r => Collected(r.getString(0), r.getString(1), r.getString(2),
          if (r.isNullAt(3)) null else Boolean.box(r.getBoolean(3)), r.getString(4)))
      captured.synchronized {
        captured += MicroBatch(batchId, fm.flatMap(_.eventTimeWatermarkForLateEvents),
          fm.flatMap(_.eventTimeWatermarkForEviction), rows)
      }
    }
    val query = job.logic(stream.toDF(), descriptor).writeStream
      .outputMode("append")
      .option("checkpointLocation", checkpoint.toString)
      .foreachBatch(sink)
      .start()
    execRef.set(query.asInstanceOf[StreamingQueryWrapper].streamingQuery)

    val plan: Seq[(Int, Seq[ReplayRow])] =
      (1 until fixture.flushBatch).map(b => b -> events.filter(e => batchOf(e.arrival_seq) == b)) ++
        Seq(fixture.flushBatch -> Seq(control), (fixture.flushBatch + 1) -> Seq.empty[ReplayRow])

    val runs = mutable.ArrayBuffer[BatchRun]()
    var seenProgress = 0
    for ((b, rows) <- plan) {
      val before = captured.size
      if (rows.nonEmpty) stream.addData(rows)
      query.processAllAvailable()
      val all = query.recentProgress
      // Idle triggers (no batch executed) also report progress; keep only executed micro-batches.
      val executed = all.drop(seenProgress).toSeq.filter(_.stateOperators.nonEmpty)
      runs += BatchRun(b, rows, executed, captured.slice(before, captured.size).toSeq)
      seenProgress = all.length
    }
    val allProgress = query.recentProgress.toSeq.filter(_.stateOperators.nonEmpty)
    query.stop()

    def wmOf(p: StreamingQueryProgress): Long =
      Option(p.eventTime.get("watermark")).map(s => Instant.parse(s).toEpochMilli).getOrElse(0L)
    def dropped(p: StreamingQueryProgress): Long = p.stateOperators.map(_.numRowsDroppedByWatermark).sum
    def emittedRecords(rows: Seq[Collected]): Seq[JsonNode] =
      rows.filter(r => r.kind == "record" && r.vin != LegacyJob.ReservedVin).map(r => mapper.readTree(r.recordJson))
    val finalWm = allProgress.lastOption.map(wmOf).getOrElse(0L)

    // Trace: one line per fixture batch.
    val trace = mutable.ArrayBuffer[ObjectNode]()
    var prevAfter = 0L
    var totalHarnessLate = 0L
    val reachedState = mutable.Set[String]()
    for ((run, i) <- runs.zipWithIndex) {
      val inEffect = run.progresses.headOption.map(wmOf).getOrElse(prevAfter)
      val after = runs.drop(i + 1).flatMap(_.progresses).headOption.map(wmOf).getOrElse(finalWm)
      val sparkDropped = run.progresses.map(dropped).sum
      val harnessLate = run.rows.count(r => validTs.get(r.arrival_seq).exists(ts => inEffect > 0 && ts <= inEffect)).toLong
      totalHarnessLate += harnessLate
      run.rows.filter(r => validTs.get(r.arrival_seq).exists(ts => !(inEffect > 0 && ts <= inEffect)))
        .foreach(r => reachedState += vinOf(r.arrival_seq))
      if (sparkDropped != harnessLate)
        failures += s"batch ${run.batch}: numRowsDroppedByWatermark $sparkDropped != harness late $harnessLate"
      val line = mapper.createObjectNode()
      line.put("batch", run.batch)
      line.put("kind",
        if (run.batch < fixture.flushBatch) "events" else if (run.batch == fixture.flushBatch) "flush_control" else "flush_empty")
      val seqs = line.putArray("arrival_seqs")
      run.rows.foreach(r => seqs.add(r.arrival_seq))
      line.put("watermark_in_effect", inEffect)
      line.put("watermark_after", after)
      line.put("num_rows_dropped_by_watermark", sparkDropped)
      line.put("harness_late", harnessLate)
      val emitted = line.putArray("records_emitted")
      emittedRecords(run.mbs.flatMap(_.rows)).foreach(n => emitted.add(n))
      val mbNodes = line.putArray("microbatches")
      for (p <- run.progresses) {
        val mb = run.mbs.find(_.batchId == p.batchId)
        val n = mbNodes.addObject()
        n.put("batch_id", p.batchId)
        n.put("num_input_rows", p.numInputRows)
        n.put("watermark", wmOf(p))
        mb.flatMap(_.lateFilterWm).foreach(w => n.put("late_filter_watermark", w))
        mb.flatMap(_.evictionWm).foreach(w => n.put("eviction_watermark", w))
        n.put("num_rows_dropped_by_watermark", dropped(p))
        n.put("num_state_rows_total", p.stateOperators.map(_.numRowsTotal).sum)
        val ids = n.putArray("output_ids")
        emittedRecords(mb.toSeq.flatMap(_.rows)).foreach(r => ids.add(r.get("output_id").asText()))
      }
      trace += line
      prevAfter = after
    }

    // Outputs and counters, from the job's own rows (reserved VIN excluded).
    val allRows = captured.flatMap(_.rows).toSeq
    if (allRows.exists(r => r.vin == LegacyJob.ReservedVin && r.kind != "state"))
      failures += "reserved VIN produced records or counters"
    val rows = allRows.filter(_.vin != LegacyJob.ReservedVin)
    val records = emittedRecords(rows)
    val counterCounts = rows.filter(_.kind == "counter").groupBy(_.counter).map { case (k, v) => k -> v.size.toLong }
    val sparkLate = LegacyJob.droppedByWatermark(allProgress)
    if (sparkLate != totalHarnessLate)
      failures += s"total numRowsDroppedByWatermark $sparkLate != harness late $totalHarnessLate"
    val counters = normalize(job.counters(counterCounts, sparkLate))

    // Drain check (SPEC.md, replay section): every VIN's last status marker is busy = false except
    // the reserved VIN's (busy = true); numRowsTotal in the last progress = VINs that reached state + 1.
    val markers = captured.toSeq.flatMap(mb =>
      mb.rows.filter(_.kind == "state").map(r => (r.vin, mb.batchId, r.busy.booleanValue())))
    val lastMarker = markers.groupBy(_._1).map { case (v, ms) => v -> ms.last }
    val busyOthers = lastMarker.values.filter(m => m._3 && m._1 != LegacyJob.ReservedVin).map(_._1).toSeq.sorted
    val expectedStateRows = (reachedState - LegacyJob.ReservedVin).size + 1L
    val stateRowsTotal = allProgress.lastOption.map(_.stateOperators.map(_.numRowsTotal).sum).getOrElse(-1L)
    val drainFailures = mutable.ArrayBuffer[String]()
    if (finalWm != fixture.target) drainFailures += s"final watermark $finalWm != target ${fixture.target}"
    if (busyOthers.nonEmpty) drainFailures += s"VINs still busy after the flush: ${busyOthers.mkString(",")}"
    if (!lastMarker.get(LegacyJob.ReservedVin).exists(_._3)) drainFailures += "reserved VIN's last marker is not busy = true"
    if (stateRowsTotal != expectedStateRows)
      drainFailures += s"numRowsTotal $stateRowsTotal in the last progress != VINs that reached state + 1 ($expectedStateRows)"
    val drain = mapper.createObjectNode()
    drain.put("passed", drainFailures.isEmpty)
    drain.put("final_watermark", finalWm)
    drain.put("target_watermark", fixture.target)
    drain.put("state_rows_total_last_progress", stateRowsTotal)
    drain.put("vins_reached_state_plus_reserved", expectedStateRows)
    val lm = drain.putArray("last_markers")
    lastMarker.toSeq.sortBy(_._1).foreach { case (_, (v, b, busy)) =>
      val n = lm.addObject(); n.put("vin", v); n.put("batch", b); n.put("busy", busy)
    }
    val dfs = drain.putArray("failures")
    drainFailures.foreach(f => dfs.add(f))
    if (drainFailures.nonEmpty)
      failures += s"drain check failed (${drainFailures.mkString("; ")}); parity refuses to compare"

    ReplayOutcome(job, records, counters, trace.toSeq, drain, failures.toSeq, drainFailures.toSeq)
  }

  def runCase(spark: SparkSession, descriptor: String, messages: Map[String, Descriptors.Descriptor],
              caseDir: Path, out: Path): ObjectNode = {
    val expected = mapper.readTree(caseDir.resolve("expected.json").toFile)
    val job = Jobs(expected.get("job").asText())
    val fixture = readFixture(caseDir.resolve("input.jsonl"))
    val outcome = runReplay(spark, descriptor, messages, job, fixture)
    val failures = mutable.ArrayBuffer[String]()
    failures ++= outcome.failures

    // Compare, unless the drain check failed.
    val refused = outcome.drainFailures.nonEmpty
    if (!refused) {
      failures ++= compareRecords(expected.get("records"), outcome.records)
      if (expected.get("counters") != outcome.counters)
        failures += s"counters: expected ${expected.get("counters")} got ${outcome.counters}"
      Option(expected.get("watermarks")).foreach(_.fields().asScala.foreach { e =>
        val got = outcome.trace.find(_.get("batch").asInt() == e.getKey.toInt).map(_.get("watermark_after").asLong())
        if (!got.contains(e.getValue.asLong()))
          failures += s"watermark after batch ${e.getKey}: expected ${e.getValue} got ${got.getOrElse("none")}"
      })
    }

    val outputs = mapper.createObjectNode()
    outputs.put("job", job.name)
    val recs = outputs.putArray("records")
    outcome.records.sortBy(_.get("output_id").asText()).foreach(r => recs.add(r))
    outputs.set[JsonNode]("counters", outcome.counters)
    write(out.resolve("outputs.json"), pretty(outputs))
    write(out.resolve("trace.jsonl"), traceText(outcome.trace))
    write(out.resolve("drain.json"), pretty(outcome.drain))

    val result = mapper.createObjectNode()
    result.put("status", if (refused) "REFUSED" else if (failures.isEmpty) "PASS" else "FAIL")
    result.put("job", job.name)
    result.set[JsonNode]("drain", outcome.drain)
    val fs = result.putArray("failures")
    failures.foreach(f => fs.add(f))
    write(out.resolve("result.json"), pretty(result))
    result
  }

  /** Whole-job fixture mode: replay <path> for <jobName> and write <job>.records.jsonl,
    * <job>.counters.json, <job>.trace.jsonl, <job>.drain.json, <job>.result.json and
    * <job>.run.json under `out`. No expected.json comparison; the result is PASS iff
    * the harness's own checks (flush target, late counts, reserved VIN, drain) hold.
    */
  def runFixture(spark: SparkSession, descriptor: String, messages: Map[String, Descriptors.Descriptor],
                 jobName: String, fixturePath: Path, out: Path): String = {
    val job = Jobs(jobName)
    val fixture = readFixture(fixturePath)
    val outcome = runReplay(spark, descriptor, messages, job, fixture)
    val status = if (outcome.failures.isEmpty) "PASS" else "FAIL"

    val recordsText = outcome.records.sortBy(_.get("output_id").asText())
      .map(r => mapper.writeValueAsString(r)).mkString("", "\n", "\n")
    write(out.resolve(s"${job.name}.records.jsonl"), recordsText)
    write(out.resolve(s"${job.name}.counters.json"), pretty(outcome.counters))
    write(out.resolve(s"${job.name}.trace.jsonl"), traceText(outcome.trace))
    write(out.resolve(s"${job.name}.drain.json"), pretty(outcome.drain))

    val result = mapper.createObjectNode()
    result.put("job", job.name)
    result.put("status", status)
    val fs = result.putArray("failures")
    outcome.failures.foreach(f => fs.add(f))
    write(out.resolve(s"${job.name}.result.json"), pretty(result))

    val conf = mapper.createObjectNode()
    Seq("spark.master", "spark.sql.shuffle.partitions", "spark.sql.session.timeZone",
      "spark.sql.streaming.noDataMicroBatches.enabled")
      .foreach(k => conf.put(k, spark.conf.get(k)))
    val replay = mapper.createObjectNode()
    replay.put("spark_version", spark.version)
    replay.put("scala_version", scala.util.Properties.versionNumberString)
    replay.put("java_version", System.getProperty("java.version"))
    replay.set[JsonNode]("spark_conf", conf)
    val run = mapper.createObjectNode()
    run.put("job", job.name)
    run.put("implementation", "spark-legacy")
    run.put("fixture_sha256", sha256Hex(Files.readAllBytes(fixturePath)))
    val topics = run.putArray("output_topics")
    topics.add(job.outputTopic)
    run.put("watermark_delay_ms", job.delayMs)
    run.set[JsonNode]("replay", replay)
    write(out.resolve(s"${job.name}.run.json"), pretty(run))
    status
  }

  def traceText(trace: Seq[ObjectNode]): String =
    trace.map(n => mapper.writeValueAsString(n)).mkString("", "\n", "\n")

  def sha256Hex(bytes: Array[Byte]): String =
    MessageDigest.getInstance("SHA-256").digest(bytes).map(b => f"${b & 0xff}%02x").mkString

  def compareRecords(expected: JsonNode, actual: Seq[JsonNode]): Seq[String] = {
    val problems = mutable.ArrayBuffer[String]()
    val byId = actual.groupBy(_.get("output_id").asText())
    byId.filter(_._2.size > 1).keys.foreach(id => problems += s"output_id $id emitted ${byId(id).size} times")
    val exp = expected.elements().asScala.map(r => r.get("output_id").asText() -> r).toMap
    (exp.keySet -- byId.keySet).foreach(id => problems += s"missing record $id")
    (byId.keySet -- exp.keySet).foreach(id => problems += s"unexpected record ${byId(id).head}")
    for ((id, e) <- exp; a <- byId.get(id).map(_.head)) {
      val names = (e.fieldNames().asScala ++ a.fieldNames().asScala).toSet
      for (f <- names) {
        val (ev, av) = (e.get(f), a.get(f))
        val ok =
          if (ev == null || av == null) false
          else if (ToleranceFields(f) && ev.isNumber && av.isNumber)
            math.abs(ev.asDouble() - av.asDouble()) <= AvgTolerance
          else if (ev.isNumber && av.isNumber) {
            if (ev.isIntegralNumber && av.isIntegralNumber) ev.asLong() == av.asLong()
            else ev.asDouble() == av.asDouble()
          } else ev == av
        if (!ok) problems += s"record $id field $f: expected $ev got $av"
      }
    }
    problems
  }

  def readFixture(path: Path): Fixture = {
    val lines = Files.readAllLines(path, StandardCharsets.UTF_8).asScala.map(l => mapper.readTree(l))
    val flush = lines.last
    val rows = lines.init.map(l =>
      FixtureRow(l.get("arrival_seq").asLong(), l.get("batch").asInt(), l.get("message").toString))
    Fixture(rows, flush.get("advance_watermark_to").asLong(), flush.get("batch").asInt())
  }

  def encode(desc: Descriptors.Descriptor, json: String): Array[Byte] = {
    val b = DynamicMessage.newBuilder(desc)
    JsonFormat.parser().merge(json, b)
    b.build().toByteArray
  }

  def loadDescriptors(path: String): Map[String, Descriptors.Descriptor] = {
    val set = DescriptorProtos.FileDescriptorSet.parseFrom(Files.readAllBytes(Paths.get(path)))
    val protos = set.getFileList.asScala.map(p => p.getName -> p).toMap
    val built = mutable.Map[String, Descriptors.FileDescriptor]()
    def build(name: String): Descriptors.FileDescriptor = built.get(name) match {
      case Some(fd) => fd
      case None =>
        val p = protos(name)
        val fd = Descriptors.FileDescriptor.buildFrom(p, p.getDependencyList.asScala.map(build).toArray)
        built.put(name, fd)
        fd
    }
    protos.keys.foreach(build)
    built.values.flatMap(_.getMessageTypes.asScala).map(d => d.getFullName -> d).toMap
  }

  def normalize(v: Any): JsonNode = mapper.readTree(mapper.writeValueAsString(v))
  def pretty(n: JsonNode): String = mapper.writer(SerializationFeature.INDENT_OUTPUT).writeValueAsString(n) + "\n"
  def write(p: Path, s: String): Unit = {
    Files.createDirectories(p.getParent)
    Files.write(p, s.getBytes(StandardCharsets.UTF_8))
  }
}
