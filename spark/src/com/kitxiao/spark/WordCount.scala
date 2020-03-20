package com.kitxiao.spark;
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object WordCount {
  def main(args: Array[String]): Unit = {
      val inputFile = "file:///tmp/data/xiao.txt"
      val conf = new SparkConf().setAppName("wordcount").setMaster("spark://master:7077")
      val sc = new SparkContext(conf)
      val textFile = sc.textFile(inputFile)
      val wordcount = textFile.flatMap(line => line.split(" ")).map(word => (word,1)).reduceByKey((a,b)=>(a+b))
      wordcount.foreach(println)     
  }
}
