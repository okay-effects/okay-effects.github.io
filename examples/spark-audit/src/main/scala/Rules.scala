import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.*
import scala.jdk.CollectionConverters.*
import okay.effects.*
import okay.effects.std.*

final case class Trade(account: String, amount: Long)

// a business rule, written once with effects: a raise refuses, a tell notes
type Check[X] = (Error % String + Writer % String)[X]

object rules extends std.On[Check]:
  def check(t: Trade): Trade ! Check =
    for
      _ <- if t.amount <= 0 then raise(s"${t.account}: amount ${t.amount} refused") else pure(())
      _ <- if t.amount > 10000 then tell(s"${t.account}: large amount ${t.amount}") else pure(())
    yield t

/** the rule handled: each record's outcome a value — refused, or passed with its notes */
def outcome(t: Trade): Either[String, (List[String], Trade)] = run(attempt[String](writer[String](rules.check(t))))

@main def rulesOnSpark(): Unit =
  // the same rule, in a plain test, no Spark
  println("local: " + outcome(Trade("acct-1", -5)) + " " + outcome(Trade("acct-2", 20000)))

  val spark = SparkSession.builder().master("local[2]").appName("rules").config("spark.ui.enabled", "false").getOrCreate()
  val trades = spark.createDataFrame(Seq(Row("acct-1", 100L), Row("acct-2", 25000L), Row("acct-3", -7L), Row("acct-4", 40L)).asJava,
    StructType(Seq(StructField("account", StringType), StructField("amount", LongType))))

  // on Spark: the rule runs in the tasks, on the executors, one program per record
  val checked = trades.rdd.map(r => Trade(r.getString(0), r.getLong(1))).map(outcome)
  val (refused, passed) = checked.collect().partition(_.isLeft)
  println("refused: " + refused.map(_.left.toOption.get).mkString("; "))
  println("notes:   " + passed.flatMap(_.toOption.get._1).mkString("; "))
  println("passed:  " + passed.map(_.toOption.get._2.account).mkString(", "))
  spark.stop()
