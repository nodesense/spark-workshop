package ai.nodesense.streams

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.OutputMode

object Stream01_WordCount {

  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder
      .master("local")
      .appName("example")
      .getOrCreate()

    sparkSession.sparkContext.setLogLevel("WARN")

    //create stream from socket

    val socketStreamDf = sparkSession.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 55555)
      .load()

    import sparkSession.implicits._
    val socketDs = socketStreamDf.as[String]
    val wordsDs =  socketDs.flatMap(value => value.split("""\s+"""))
                           .map ( word => word.trim)
                           .filter (word => !word.isEmpty)

    val countDs = wordsDs.groupBy("value").count()

    val query = countDs.writeStream
      //.outputMode("append")
      .format("console")
      .outputMode(OutputMode.Complete())
      .start()

    query.awaitTermination()
  }
}