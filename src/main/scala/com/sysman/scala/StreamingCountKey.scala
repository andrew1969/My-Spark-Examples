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


object StreamingCountKey {
  def main(args: Array[String]) {
    val logDirectory = "E:\\Spark\\MyEx\\FLogs"
    val outDir = "E:\\Spark\\MyEx\\outDir\\CountByState"
    val conf = new SparkContext(new SparkConf()
                                .setAppName("Spark Stream Log Input"))
//                                .set("spark.local.dir", "E:\\SPARK\\MyEx\\TMP_LOCAL"))
    val ssc = new StreamingContext(conf, Seconds(5))
    ssc.checkpoint("E:\\SPARK\\MyEx\\TMP")
    val logData = ssc.textFileStream(logDirectory)
    val accessLogDStream = logData.map(line => ApacheAccessLog.parseLogLine(line))
    val responseCodeDStream = accessLogDStream.map(log => (log.getResponseCode(),1L))
    val responseCodeCountDStream = responseCodeDStream.updateStateByKey(updateRunningSum _)
    responseCodeCountDStream.saveAsTextFiles(outDir,"txt")

    ssc.start()
    ssc.awaitTermination(60000)

  }
  def updateRunningSum(values: Seq[Long], state:Option[Long]) = {
    Some(state.getOrElse(0L) + values.size)
  }
}
