/** Contenggio in Saprk Streaming delle occorrenze di un log APACHE contenenti **/
/** lo stesso indirizzo IP.            Versione Incrementale                   **/
/** Learning Spark pp 194 - 195                                                **/

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


object StreamingCountInc {
  def main(args: Array[String]) {
    val logDirectory = "E:\\Spark\\MyEx\\FLogs"
    val outDir = "E:\\Spark\\MyEx\\outDir\\CountLog"
    val conf = new SparkContext(new SparkConf().setAppName("Spark Stream Log Input"))
    val ssc = new StreamingContext(conf, Seconds(5))
    ssc.checkpoint("E:\\SPARK\\MyEx\\TMP")
    val logData = ssc.textFileStream(logDirectory)
    val accessLogDStream = logData.map(line => ApacheAccessLog.parseLogLine(line))
    val ipDStream = accessLogDStream.map(logEntry => (logEntry.getIpAddress(), 1))
    
    // il conteggio avviene non sulla singola window ma su quelle rientranti nella finestra di esecuzione
    val ipCountDStream = ipDStream.reduceByKeyAndWindow(
                        {(x,y) => x + y}, //Elabora i batch che entrano nella finestra di esecuzione
                        {(x,y) => x - y}, //Rimuove gli elementi del più vecchio batch uscente dalla finestra di esecuzione                                )
                        Seconds(30),      //Durata della finestra di esecuzione
                        Seconds(10))     //Durata dello Slide.
    ipCountDStream.saveAsTextFiles(outDir,"txt")
    ssc.start()
    ssc.awaitTermination()

  }
}
