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


object StreamingCount {
  def main(args: Array[String]) {
    val logDirectory = "E:\\Spark\\MyEx\\FLogs"
    val outDir = "E:\\Spark\\MyEx\\outDir\\CountLog"
    val conf = new SparkContext(new SparkConf().setAppName("Spark Stream Log Input"))
    val ssc = new StreamingContext(conf, Seconds(5))

    val logData = ssc.textFileStream(logDirectory)
    val accessLogDStream = logData.map(line => ApacheAccessLog.parseLogLine(line))
    val ipDStream = accessLogDStream.map(entry => (entry.getIpAddress(), 1))
    val ipCountDStream = ipDStream.reduceByKey((x,y) => x + y)

    //val errorLines = lines.filter(_.contains("DELETE"))
    //ipDStream.print()
    ipCountDStream.saveAsTextFiles(outDir,"txt")
    //System.out.println(errorLines)
    ssc.start()
    ssc.awaitTermination()

  }
}
