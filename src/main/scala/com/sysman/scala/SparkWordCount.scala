/** Conta le parole nel file in input (args[0] e filtra quelle il cui numero é  **/
/** maggiore della soglia impostata in input (args[1])                          **/
/**                               NO STREAMING                                  **/
package com.sysman.scala
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf


object SparkWordCount {
  def main(args: Array[String]) {
    
    // crea Spark context. 
    val sc = new SparkContext(new SparkConf().setAppName("Spark Count"))

    // legge la soglia in input.
    val threshold = args(1).toInt

    // legge il testo in input e lo suddivide nelle parole componenti.
    // --> usa flatMap per creare un rdd non delimitato da \n
    val tokenized = sc.textFile(args(0)).flatMap(_.split(" "))

    // conta le occorrenze di ogni parola.
    // --> 1) usa map per creare un rdd di coppie (parola,contatore) 
    // --> 2) usa reduceByKey per contare le occorrenze di ogni "parola" in "contatore"
    val wordCounts = tokenized.map((_, 1)).reduceByKey(_ + _)

    // filtra le parole con un numero di occorrenze minore della soglia
    // --> usa filter per filtrare quelle coppie (parola,contatore) che hanno "contatore > soglia
    val filtered = wordCounts.filter(_._2 >= threshold)

    // conta i caratteri
    // --> 1) flatMap per ridurre le occorrenze residue in un array di caratteri
    // --> 2) map per creare coppie (carattere,contatore)
    // --> 2) reduceByKey per contare le occorrenze di ogni carattere.
    val charCounts = filtered.flatMap(_._1.toCharArray).map((_, 1)).reduceByKey(_ + _)
    
    // Mostra a terminale i risultati.
    // --> collect è l'action che provoca l'esecuzione di tutte le transformation inserite in precedenza.
    System.out.println(charCounts.collect().mkString(", "))
  }
}
