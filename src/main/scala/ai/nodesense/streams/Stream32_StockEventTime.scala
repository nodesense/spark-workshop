package ai.nodesense.streams

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.Date

import ai.nodesense.models.Order.dateFormat
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.OutputMode

object Stream32_StockEventTime {

  case class Stock(time: Timestamp, symbol: String, value: Double)


  val dateFormat:SimpleDateFormat = new SimpleDateFormat(
    "yyyy-mm-dd");

  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder
      .master("local")
      .appName("stockeventtime-app")
      .getOrCreate()
    //create stream from socket

    import sparkSession.implicits._
    sparkSession.sparkContext.setLogLevel("ERROR")
    val socketStreamDs = sparkSession.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 55555)
      .load()
      .as[String]

    // read as stock
    val stockDs = socketStreamDs.map(value => {
      val columns = value.split(",")

      val parsedTimeStamp: Date = dateFormat.parse(columns(0));

      val timestamp: Timestamp = new Timestamp(parsedTimeStamp.getTime());

      //Stock(new Timestamp(columns(0).toLong), columns(1), columns(2).toDouble)
      Stock(timestamp, columns(1), columns(2).toDouble)
    })

    val windowedCount = stockDs
      .groupBy(
        window($"time", "10 seconds")
      )
      .sum("value")


    val query =
      windowedCount.writeStream
        .format("console")
        .option("truncate", "false")
        .outputMode(OutputMode.Complete())


    query.start().awaitTermination()
  }
}