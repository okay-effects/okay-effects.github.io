import java.nio.charset.StandardCharsets.UTF_8
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions.*
import okay.effects.*
import okay.spark.SparkEvidence
import okay.spark.SparkEvidence.{Canonical, Committed, Manifest, Receipt}

// ---- the existing job, as it is today ----
def dailyTotalsToday(spark: SparkSession, lake: String, day: String): Unit =
  val trades = spark.read.parquet(s"$lake/trades/day=$day")
  val totals = trades.groupBy("account").agg(sum("amount").as("total"))
  totals.write.mode("overwrite").parquet(s"$lake/totals/day=$day")

// ---- step 1: evidence of the output, the logic untouched ----
val rowBytes: Canonical[Row] = row => row.mkString("\u001f").getBytes(UTF_8)
def stable(df: DataFrame): DataFrame = df.repartition(8, col("account")).sortWithinPartitions("account")

def dailyTotalsWithEvidence(spark: SparkSession, lake: String, day: String): Committed =
  val trades = spark.read.parquet(s"$lake/trades/day=$day")
  val totals = stable(trades.groupBy("account").agg(sum("amount").as("total")))
  val out = s"$lake/totals/day=$day"
  totals.write.mode("overwrite").parquet(out)
  SparkEvidence.run(totals.rdd, runId = s"totals-$day")(rowBytes) { m =>
    // the manifest beside the data; in production a ledger, a bucket with object lock, a signed record
    java.nio.file.Files.writeString(java.nio.file.Path.of(s"$lake/totals-$day.evidence"), s"${m.digest} ${m.partitions.map(_.count).sum}")
    Receipt(output = out, acknowledgement = m.digest)
  }

/** later, by anyone: the stored output read back, its digest recomputed and compared */
def verify(spark: SparkSession, out: String, runId: String, expected: String): Boolean =
  SparkEvidence.run(stable(spark.read.parquet(out)).rdd, runId)(rowBytes)(m => Receipt(out, m.digest)).manifest.digest == expected

// ---- step 2: reads and writes as effects; the audit a handler around them ----
enum Lake[X]:
  case Read(path: String) extends Lake[DataFrame]
  case Write(df: DataFrame, path: String) extends Lake[Unit]

def read(path: String): DataFrame ! Lake = perform[Lake](Lake.Read(path))
def write(df: DataFrame, path: String): Unit ! Lake = perform[Lake](Lake.Write(df, path))

/** the same job: its logic as it was, its reads and writes now operations */
def dailyTotals(lake: String, day: String): Unit ! Lake =
  for
    trades <- read(s"$lake/trades/day=$day")
    totals = trades.groupBy("account").agg(sum("amount").as("total"))
    _ <- write(stable(totals), s"$lake/totals/day=$day")
  yield ()

/** the handler doing the I/O: Spark, as before */
def onSpark[R[_], Rest[_], A](spark: SparkSession)(p: A ! R)(using Split[Lake, R, Rest]): A ! Rest =
  handle(p, new TailHandler[Lake, Rest, A, A]:
    def ret(a: A) = pure(a)
    def answer[X](op: Lake[X]): X = op match
      case Lake.Read(path) => spark.read.parquet(path)
      case Lake.Write(df, path) => df.write.mode("overwrite").parquet(path))

trait AuditLog:
  def append(line: String): Unit

/** the audit: every read and write recorded, intent first; each output with its evidence */
def audited[R[_], A](log: AuditLog, runId: String)(p: A ! R)(using In[Lake, R]): A ! R =
  intercept(p, new Handler[Lake, R, A, A]:
    def ret(a: A) = pure(a)
    def op[X](op: Lake[X], k: X => A ! R) = op match
      case Lake.Read(path) =>
        log.append(s"read  $path")
        perform[Lake].apply[R, X](op).flatMap { df =>
          log.append(s"read  $path: ${df.count()} rows, ${df.schema.simpleString}"); k(df) }
      case Lake.Write(df, path) =>
        log.append(s"write $path")
        perform[Lake].apply[R, X](op).flatMap { done =>
          val e = SparkEvidence.run(df.rdd, runId)(rowBytes)(m => Receipt(path, m.digest)).manifest
          log.append(s"write $path: ${e.partitions.map(_.count).sum} rows, digest ${e.digest.take(16)}…"); k(done) })

@main def check(): Unit =
  val spark = SparkSession.builder().master("local[2]").appName("audit").config("spark.ui.enabled", "false").getOrCreate()
  import org.apache.spark.sql.types.*
  import scala.jdk.CollectionConverters.*
  def frame(cols: (String, String), rows: (String, Long)*) = spark.createDataFrame(
    rows.map((a, n) => Row(a, n)).asJava, StructType(Seq(StructField(cols._1, StringType), StructField(cols._2, LongType))))
  val lake = "/tmp/spark-audit/lake"; val day = "2026-10-18"
  frame(("account", "amount"), ("acct-1", 100L), ("acct-2", 250L), ("acct-1", 40L), ("acct-3", 5L))
    .write.mode("overwrite").parquet(s"$lake/trades/day=$day")

  dailyTotalsToday(spark, lake, day)
  println("today: " + spark.read.parquet(s"$lake/totals/day=$day").orderBy("account").collect().mkString(" "))

  val c = dailyTotalsWithEvidence(spark, lake, day)
  println(s"evidence: ${c.manifest.partitions.map(_.count).sum} rows, ${c.manifest.partitions.size} partitions, digest ${c.manifest.digest.take(16)}…")
  println("verify as stored: " + verify(spark, s"$lake/totals/day=$day", s"totals-$day", c.manifest.digest))
  frame(("account", "total"), ("acct-9", 1L)).write.mode("append").parquet(s"$lake/totals/day=$day")
  println("verify after a row was added: " + verify(spark, s"$lake/totals/day=$day", s"totals-$day", c.manifest.digest))

  val lines = collection.mutable.ListBuffer.empty[String]
  val log: AuditLog = line => lines += line
  run(onSpark(spark)(audited(log, s"totals-$day")(dailyTotals(lake, day))))
  lines.foreach(l => println("audit: " + l))
  spark.stop()
