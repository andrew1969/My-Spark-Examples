/** Contenggio in Saprk Streaming delle occorrenze di un log APACHE contenenti **/
/** lo stesso indirizzo IP. Versione StateFull                                 **/
/** Learning Spark pp 196 - 197                                                **/

package com.sysman.scala

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.Duration
import org.apache.spark.streaming.Seconds


object StreamingCountKey {
  def main(args: Array[String]) {
    val logDirectory = "E:\\Spark\\MyEx\\FLogs"
    val outDir = "E:\\Spark\\MyEx\\outDir\\CountByState"
    val conf = new SparkContext(new SparkConf().setAppName("Spark Stream Log Input"))
    val ssc = new StreamingContext(conf, Seconds(5))
    
    // impostazione checkPoint
    // --> necessaria in caso di crash/ripartenze
    ssc.checkpoint("E:\\SPARK\\MyEx\\TMP")
    val logData = ssc.textFileStream(logDirectory)
    val accessLogDStream = logData.map(line => ApacheAccessLog.parseLogLine(line))
    val responseCodeDStream = accessLogDStream.map(log => (log.getResponseCode(),1L))
    
    // Aggiorna la variabile di stato che conta le occorrenze di ciascun response Code tramite la funzione 
    // di update dello stato.
    val responseCodeCountDStream = responseCodeDStream.updateStateByKey(updateRunningSum _)
    responseCodeCountDStream.saveAsTextFiles(outDir,"txt")

    ssc.start()
    ssc.awaitTermination()

  }
  def updateRunningSum(values: Seq[Long], state:Option[Long]) = {
    Some(state.getOrElse(0L) + values.size)
  }
}
