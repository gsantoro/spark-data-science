import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by gsantoro on 19/10/15.
 */
object Main {
  def main(args: Array[String]) {
    val dataPath = "src/main/resources/chap02/hdfs/linkage"
    val sparkConf = new SparkConf().setAppName("Hello world")
    val sc = new SparkContext(sparkConf)

    val rawBlocks = sc.textFile(dataPath, 2)

    val head = rawBlocks.take(10)

    val noheader = head
      .filterNot(isHeader)

    noheader.map(parse).foreach(println)
  }

  def parse(line: String): MatchData = {
    val pieces = line.split(",")
    val id1 = pieces(0).toInt
    val id2 = pieces(1).toInt
    val scores = pieces.slice(2, 11).map(toDouble)
    val matched = pieces(11).toBoolean
    MatchData(id1, id2, scores, matched)
  }

  case class MatchData(id1: Int, id2: Int, scores: Array[Double], matched: Boolean)

  def toDouble(s: String): Double = if ("?".equals(s)) Double.NaN else s.toDouble

  def isHeader(line: String): Boolean = {
    line.contains("id_1")
  }
}
