package ai.nodesense.streams

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger

object CountingInAStreamDatasetExpGroupBy {
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  def main(args:Array[String]): Unit = {
    val host = args(0)
    val port = args(1)
    val checkpointFolder = args(2)

    val isLocal = true

    val sparkSession = if (isLocal) {
      SparkSession.builder
        .master("local")
        .appName("my-spark-app")
        .config("spark.some.config.option", "config-value")
        .config("spark.driver.host","127.0.0.1")
        .config("spark.sql.parquet.compression.codec", "gzip")
        .master("local[3]")
        .getOrCreate()
    } else {
      SparkSession.builder
        .appName("my-spark-app")
        .config("spark.some.config.option", "config-value")
        .master("local[3]")
        .getOrCreate()
    }

    import sparkSession.implicits._

    val socketLines = sparkSession.readStream
      .format("socket")
      .option("host", host)
      .option("port", port)
      .load()

    val messageDs = socketLines.as[String].map(line => {
      MessageBuilder.build(line)
    }).as[Message]

    val tickerCount = messageDs.groupBy("ticker", "destUser").agg(sum($"price"), avg($"price"))

    val ticketOutput = tickerCount.writeStream
      .format("Console")
      .trigger(Trigger.ProcessingTime("5 seconds"))
      .option("checkpointLocation", checkpointFolder)
      .outputMode("complete")
      .format("console")
      .start()

    ticketOutput.awaitTermination()
  }
}
