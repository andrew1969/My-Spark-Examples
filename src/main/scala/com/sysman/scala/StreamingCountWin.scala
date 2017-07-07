package com.sysman.scala

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
//import ApacheAccessLog
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.Duration
import org.apache.spark.streaming.Seconds


object StreamingCountWin {
  def main(args: Array[String]) {
    val logDirectory = "E:\\Spark\\MyEx\\FLogs"
    val outDir = "E:\\Spark\\MyEx\\outDir\\CountLog"
    val conf = new SparkContext(new SparkConf().setAppName("Spark Stream Log Input"))
    val ssc = new StreamingContext(conf, Seconds(5))
    ssc.checkpoint("E:\\SPARK\\MyEx\\TMP")
    val logData = ssc.textFileStream(logDirectory)
    val accessLogDStream = logData.map(line => ApacheAccessLog.parseLogLine(line))
    val accessLogWindow = accessLogDStream.window(Seconds(30),Seconds(10))
    val windowCounts = accessLogWindow.count()
    windowCounts.print()
    ssc.start()
    ssc.awaitTermination()

  }
}
