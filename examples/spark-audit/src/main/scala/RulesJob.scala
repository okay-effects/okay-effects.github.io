import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types.*
import scala.jdk.CollectionConverters.*
import okay.effects.*
import okay.effects.std.*

object daily:
  // the job's own effects: the lake, and a refusal of the whole run
  type Job[X] = (Lake + Error % String)[X]

  def read(path: String): DataFrame ! Job = perform[Lake](Lake.Read(path))
  def write(df: DataFrame, path: String): Unit ! Job = perform[Lake](Lake.Write(df, path))
  def stop(why: String): Unit ! Job = raise[String, Job, Unit](why)

  private val reasons = StructType(Seq(StructField("reason", StringType)))
  private val tradesSchema = StructType(Seq(StructField("account", StringType), StructField("amount", LongType)))

  /** the rule per record on the executors; its outcomes split, decided on and written, all in the job */
  def checkTrades(lake: String, day: String, maxRefused: Long): Unit ! Job =
    for
      trades  <- read(s"$lake/trades/day=$day")
      spark    = trades.sparkSession
      checked  = trades.rdd.map(r => Trade(r.getString(0), r.getLong(1))).map(outcome).cache()
      refused  = spark.createDataFrame(checked.flatMap(_.left.toOption).map(Row(_)), reasons)
      notes    = spark.createDataFrame(checked.flatMap(_.toOption.toList.flatMap(_._1)).map(Row(_)), reasons)
      passed   = spark.createDataFrame(checked.flatMap(_.toOption.map(_._2)).map(t => Row(t.account, t.amount)), tradesSchema)
      _       <- write(refused, s"$lake/refused/day=$day")
      _       <- write(notes, s"$lake/notes/day=$day")
      n        = refused.count()
      _       <- if n > maxRefused then stop(s"$n refused, more than $maxRefused: passed trades not published")
                 else write(passed, s"$lake/passed/day=$day")
    yield ()

@main def rulesJob(): Unit =
  val spark = SparkSession.builder().master("local[2]").appName("rules-job").config("spark.ui.enabled", "false").getOrCreate()
  val lake = "/tmp/spark-audit/lake"; val day = "2026-10-18"
  spark.createDataFrame(Seq(Row("acct-1", 100L), Row("acct-2", 25000L), Row("acct-3", -7L), Row("acct-4", 40L)).asJava,
    StructType(Seq(StructField("account", StringType), StructField("amount", LongType))))
    .write.mode("overwrite").parquet(s"$lake/trades/day=$day")

  def runWith(maxRefused: Long) =
    val lines = collection.mutable.ListBuffer.empty[String]
    val log: AuditLog = line => lines += line
    val result = run(attempt[String](onSpark(spark)(audited(log, s"check-$day")(daily.checkTrades(lake, day, maxRefused)))))
    println(s"maxRefused=$maxRefused: $result")
    lines.foreach(l => println("  audit: " + l))

  runWith(maxRefused = 5)
  println("passed table: " + spark.read.parquet(s"$lake/passed/day=$day").orderBy("account").collect().mkString(" "))
  runWith(maxRefused = 0)
  spark.stop()
