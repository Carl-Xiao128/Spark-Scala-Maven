package com.kitxiao.spark
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, Row}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions._

case class DemVote(id: String, state: String, demVote: String, year: String, south: String)

case class DemVotePair(state: String, demVote: Double)

case class DemVoteResult(state: String, mean: Double, variance: Double) {
  override def toString: String = {
    state + "," + mean + "," + variance
  }
}

/**
 * Democratic vote
 */
object My extends App {

  override def main(args: Array[String]): Unit = {
    val STATE_NUM = 51
    val BOOTSTRAP_TIMES = 10 //1000
    val BOOTSTRAP_FRACTION = 0.25

    val conf = new SparkConf().setAppName("Spark and SparkSql").setMaster("spark://master:7077")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._

    val csv = sc.textFile("file:///tmp/data/presidentialElections.csv")

    //0. ETL
    val headerAndRows = csv.map(line => line.split(",").map(_.trim))
    val header = headerAndRows.first
    val voteData = headerAndRows.filter(_ (0) != header(0))

    def calculate(voteData: RDD[Array[String]]): Dataset[Row] = {
      val votePairs = voteData.map(p => DemVotePair(p(1), p(2).toDouble)).toDF
      //1. Mean and Variance
      val stateGroup = votePairs.groupBy("state")
      val stateCount = stateGroup.count().count().toInt
      //      println(stateCount)
      stateGroup
        .agg(avg("demVote").as("Mean"),
          variance("demVote").as("Variance"))
        .sort($"state".asc)
    }

    //1. calculate mean and variance
    calculate(voteData).sort("Mean").show(STATE_NUM)

    //2. sample for bootstrapping, Do 1000 times
    val sumArray = new Array[DemVoteResult](STATE_NUM)
    val sumState = new Array[String](STATE_NUM)
    val sumMean = new Array[Double](STATE_NUM)
    val sumVariance = new Array[Double](STATE_NUM)
    for (i <- 1 to BOOTSTRAP_TIMES) {
      // take 25% of the population without replacement.
      val resampledData = voteData.sample(withReplacement = false, BOOTSTRAP_FRACTION, System.currentTimeMillis())

      println("  " + i + "th calculate bootstrapping, resampled Data size is " + resampledData.count())
      val result = calculate(resampledData)

      val array: Array[Row] = result.collect()
      for (j <- array.indices) {
        if (i == 1) {
          sumState(j) = array(j)(0).toString
        }
        sumMean(j) += array(j)(1).toString.toDouble
        if (array(j)(2) != null) {
          var variance = array(j)(2).toString.toDouble
          if (variance.isNaN) {
            variance = 0
          }
          sumVariance(j) += variance
        }
      }
    }

    for (i <- sumArray.indices) {
      sumArray(i) = DemVoteResult(sumState(i), sumMean(i) / BOOTSTRAP_TIMES, sumVariance(i) / BOOTSTRAP_TIMES)
    }
    val resultFile = sc.parallelize(sumArray)
    resultFile.foreach(println)
    resultFile.saveAsTextFile("presidentialElections_result")

  }


}
