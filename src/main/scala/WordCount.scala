package org.example

import org.apache.spark.{SparkConf, SparkContext}

object WordCount {
  def main(args: Array[String]) : Unit = {
    val logFile = "hdfs:/tmp/LICENSE.txt"
    val conf = new SparkConf().setAppName("Simple Application")
    val sc = new SparkContext(conf)
    val logData = sc.textFile(logFile, 2).cache()
    val numAs = logData.filter(line => line.contains("a")).count()
    val numBs = logData.filter(line => line.contains("b")).count()
    println("Lines with a: %s, Lines with b: %s".format(numAs, numBs))
  }
}
