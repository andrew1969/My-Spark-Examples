package com.sysman.scala

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.Duration
import org.apache.spark.streaming.Seconds


object StreamingLogInput {
  def main(args: Array[String]) {
    val logDirectory = "E:\\Spark\\MyEx\\FLogs"
    val outDir = "E:\\Spark\\MyEx\\outDir"
    val conf = new SparkContext(new SparkConf().setAppName("Spark Stream Log Input"))
    val ssc = new StreamingContext(conf, Seconds(5))
    //val lines = ssc.socketTextStream("localhost",7777)

    val lines = ssc.textFileStream(logDirectory)
    val errorLines = lines.filter(_.contains("DELETE"))
    //errorLines.print()
    errorLines.saveAsTextFiles(outDir,"txt")
    //System.out.println(errorLines)
    ssc.start()
    ssc.awaitTermination()

  }
}
