/** Contenggio in Saprk Streaming delle occorrenze di un log APACHE contenenti **/
/** lo stesso indirizzo IP                                                     **/

package com.sysman.scala

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.Duration
import org.apache.spark.streaming.Seconds


object StreamingCount {
  def main(args: Array[String]) {
    // definisco le directory in cui leggere i log e scrivere i risultati
    // TBD metterle come parametri di input
    val logDirectory = "E:\\Spark\\MyEx\\FLogs"
    val outDir = "E:\\Spark\\MyEx\\outDir\\CountLog"
    
    // definisco Spark Context e Spark Streaming Context
    val conf = new SparkContext(new SparkConf().setAppName("Spark Stream Log Input"))
    val ssc = new StreamingContext(conf, Seconds(5))
    
    // Leggo i log posti in maniera "atomica" nella directory di input
    // --> atomica : copiati tramite una copy o una move e non scritti direttamente
    val logData = ssc.textFileStream(logDirectory)
    
    // Uso la classe ApacheAccessLog per eseguire il parse delle righe del log letto
    val accessLogDStream = logData.map(line => ApacheAccessLog.parseLogLine(line))
    
    // Sempre tramite ApacheAccessLog prelevo da ogni riga l'indirizzo IP
    val ipDStream = accessLogDStream.map(entry => (entry.getIpAddress(), 1))
    
    //cConto gli indirizzi IP uguali nel log corrente
    val ipCountDStream = ipDStream.reduceByKey((x,y) => x + y)

    //Salvo i risultati in un file di testo
    ipCountDStream.saveAsTextFiles(outDir,"txt")
    //System.out.println(errorLines)
    
    // Start dello streaming e attesa terminazione
    // --> provoca il loop infinito delle istruzioni precedenti
    ssc.start()
    ssc.awaitTermination()

  }
}
