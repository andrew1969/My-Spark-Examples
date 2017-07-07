/** Classe di Supporto per l'analisi di **/
/** LOG APACHE                          **/

package com.sysman.scala
import scala.io.Source

case class ApacheAccessLog(ipAddress: String,
  clientIdentd: String,
  userId: String,
  dateTime: String,
  method: String,
  endpoint: String,
  protocol: String,
  responseCode: Int,
  contentSize: Long) {

  def getIpAddress(): String = {
      return ipAddress
    }
  def getResponseCode(): Int = {
      return responseCode
    }
}

object ApacheAccessLog {
  val PATTERN = """^(\S+) (\S+) (\S+) \[([\w:/]+\s[+\-]\d{4})\] "(\S+) (\S+) (\S+)" (\d{3}) (\d+)""".r

  /**
    * Parse log entry from a string.
    *
    * @param log A string, typically a line from a log file
    * @return An entry of Apache access log
    * @throws RuntimeException Unable to parse the string
    */
  def parseLogLine(log: String): ApacheAccessLog = {
    log match {
      case PATTERN(ipAddress, clientIdentd, userId, dateTime, method, endpoint, protocol, responseCode, contentSize)
      => ApacheAccessLog(ipAddress, clientIdentd, userId, dateTime, method, endpoint, protocol, responseCode.toInt,
        contentSize.toLong)

      case _ => throw new RuntimeException(s"""Cannot parse log line: $log""")
    }
  }
  def main(args: Array[String]) {

    val filename = "E:\\Spark\\MyEx\\FLogs\\access_log_20170622-161117.log"
    for (line <- Source.fromFile(filename).getLines) {
      /*println(line)
      println ("*******")*/
      println(parseLogLine(line).getIpAddress)
    }
  }

}
