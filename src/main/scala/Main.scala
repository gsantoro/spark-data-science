import com.github.tototoshi.csv.CSVParser
import org.apache.spark.{SparkConf, SparkContext}
import org.joda.time.Duration
import org.joda.time.format.{PeriodFormatter, PeriodFormatterBuilder, PeriodFormat}

object Main {
  def main(args: Array[String]) {
    time { () =>
      val dataPath = "src/main/resources/chap02/hdfs/linkage"
      val sparkConf = new SparkConf().setAppName("Data linkage")
      val sc = new SparkContext(sparkConf)

      val rawBlocks = sc.textFile(dataPath + "/block_1.csv", 10)
//            .take(10)

      val noheader = rawBlocks
        .filter(isNotHeader)

      val parsed = noheader
        .flatMap(parse)

      val result = parsed
        .groupBy(md => md.matched)
        .mapValues(x => x.size)

      result
        .foreach(println)
    }
  }

  def time(execution: () => Unit): Unit = {
    val start = System.currentTimeMillis()

    execution()

    val end = System.currentTimeMillis()

    val duration = new Duration(end - start)
    val formatter = new PeriodFormatterBuilder()
      .appendDays()
      .appendSuffix("d")
      .appendHours()
      .appendSuffix("h")
      .appendMinutes()
      .appendSuffix("m")
      .appendSeconds()
      .appendSuffix("s")
      .toFormatter()
    val formatted = formatter.print(duration.toPeriod())
    println(s"Execution time: $formatted")
  }

  def parse(line: String): Option[MatchData] = {
    val csv = CSVParser.parse(line, '\\', ',', '"')
    csv.flatMap{ pieces =>
      val id1 = pieces.head.toInt
      val id2 = pieces(1).toInt
      val scores = pieces.slice(2, 11).map(toDouble)
      val matched = pieces(11).toBoolean
      Some(MatchData(id1, id2, scores, matched))
    }
  }

  case class MatchData(id1: Int, id2: Int, scores: List[Double], matched: Boolean)

  def toDouble(s: String): Double = if ("?".equals(s)) Double.NaN else s.toDouble

  def isNotHeader(line: String): Boolean = {
    ! line.contains("id_1")
  }
}
