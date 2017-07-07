package com.sysman.scala

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.Duration
import org.apache.spark.streaming.Seconds
import org.apache.hadoop.io.{Text,LongWritable}
import org.apache.hadoop.mapreduce.lib.input.{TextInputFormat}
import org.apache.hadoop.fs.{Path}
import org.json4s._
import org.json4s.jackson.JsonMethods._

object StreamingJson {
  def main(args: Array[String]) {
    val inputPath = "E:\\Spark\\MyEx\\Reddit"
    val outDir = "E:\\Spark\\MyEx\\outDir\\outNomi"
    val conf = new SparkContext(new SparkConf()
                                .setAppName("Spark Stream Log Input"))
//                                .set("spark.local.dir", "E:\\SPARK\\MyEx\\TMP_LOCAL"))
    val ssc = new StreamingContext(conf, Seconds(20))

    val comments = ssc.fileStream[LongWritable, Text, TextInputFormat](inputPath, (f: Path) => true, newFilesOnly=false).map(pair => pair._2.toString)

    val keyedByAuthor = comments.map(rec => ((parse(rec) \ "Nome").values.toString, (parse(rec) \ "Cognome").values.toString))
    //val filtered = keyedByAuthor.map(a,b => b)

    keyedByAuthor.saveAsTextFiles(outDir, "txt")

    ssc.start()
    ssc.awaitTermination()

  }

}
