import com.github.tototoshi.csv.CSVParser
import gs.spark.util.{SparkConfigParser, Utils}
import org.apache.spark.{SparkConf, SparkContext}

object Main {
  def main(args: Array[String]) {
    Utils.time { () =>
      val sparkConf = new SparkConf().setAppName("Data linkage")

      val conf = SparkConfigParser.parse(sparkConf)
      val sc = new SparkContext(sparkConf)

      var rawBlocks = sc.textFile(conf.input, conf.partitions)
      if (conf.debug) {
        rawBlocks = rawBlocks.sample(false, conf.sampleFraction)
      }

      val content = rawBlocks
        .filter(isNotHeader)

      val parsed = content
        .flatMap(parse)

      val result = parsed
        .groupBy(md => md.matched)
        .mapValues(x => x.size)

      if (conf.debug)
        result.foreach(println)
      else
        result.saveAsTextFile(conf.output)
    }
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
