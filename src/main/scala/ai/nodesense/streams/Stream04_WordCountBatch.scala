package ai.nodesense.streams

import java.util.concurrent.TimeUnit

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.{OutputMode, Trigger}

object Stream04_SocketMiniBatch {

  def main(args: Array[String]): Unit = {

    val sparkSession = SparkSession.builder.
      master("local")
      .appName("example")
      .getOrCreate()

    sparkSession.sparkContext.setLogLevel("WARN")


    //create stream from socket

    val socketStreamDf = sparkSession.readStream.
      format("socket")
      .option("host", "localhost")
      .option("port", 55555).load()

    import sparkSession.implicits._
    val socketDs = socketStreamDf.as[String]
    val wordsDs =  socketDs.flatMap(value => value.split("""\s+"""))
      .map ( word => word.trim)
      .filter (word => !word.isEmpty)

    val countDs = wordsDs.groupBy("value").count()


    val query = countDs.writeStream.format("console").outputMode(OutputMode.Complete()).trigger(
      Trigger.ProcessingTime(10, TimeUnit.SECONDS)
    ).start()

    query.awaitTermination()
  }

}