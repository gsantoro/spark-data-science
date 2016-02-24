import com.github.tototoshi.csv.CSVParser
import org.apache.spark.{SparkConf, SparkContext}

object Main {
  def main(args: Array[String]) {
    val dataPath = "src/main/resources/chap02/hdfs/linkage"
    val sparkConf = new SparkConf().setAppName("Hello world")
    val sc = new SparkContext(sparkConf)

    val rawBlocks = sc.textFile(dataPath, 2)

    val head = rawBlocks.take(10)

    val noheader = head
      .filterNot(isHeader)

    noheader
      .flatMap(parse)
      .foreach(println)
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

  def isHeader(line: String): Boolean = {
    line.contains("id_1")
  }
}
