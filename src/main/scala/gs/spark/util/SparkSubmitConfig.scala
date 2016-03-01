package gs.spark.util

import org.apache.spark.SparkConf

case class SparkSubmitConfig(
  input: String = "",
  output: String = "",
  partitions: Int = 2,
  sampleFraction: Double = 0.01,
  debug: Boolean = true)

object SparkConfigParser {
  def parse(conf: SparkConf): SparkSubmitConfig = {
    val config = SparkSubmitConfig(
      input = conf.get("spark.input", ""),
      output = conf.get("spark.output", ""),
      partitions = conf.getInt("spark.partitions", 2),
      sampleFraction = conf.getDouble("spark.sample.fraction", 0.01),
      debug = conf.getBoolean("spark.debug", true)
    )
    config
  }
}
