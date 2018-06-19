package ai.nodesense.streams

import ai.nodesense.models.Order
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.OutputMode

object Stream51_OrderStream {
  def main(args:Array[String]): Unit = {
    val host = "localhost"
    val port = 55555

    //var checkpointDir = args(2)

    val isLocal = true

    val sparkSession = SparkSession.builder
        .master("local")
        .appName("my-spark-app")
        .config("spark.some.config.option", "config-value")
        //.config("spark.driver.host","127.0.0.1")
        //.config("spark.sql.parquet.compression.codec", "gzip")
        //.enableHiveSupport()
        .getOrCreate()


    import sparkSession.implicits._
    import sparkSession.implicits._

    val socketLines = sparkSession.readStream
      .format("socket")
      .option("host", host)
      .option("port", port)
    //  .option("checkpointLocation", checkpointDir)
      .load()

    val messageDs = socketLines
                     .as[String]
                    .map (line => line.trim)
                    .filter(line => !line.isEmpty)
                    .map(line => {
                      Order.buildFromLine(line)
                     })
                    .filter(order => order != null)
                     .as[Order]

    val upperMessageDs = messageDs.map(message => {
      message.toString.toUpperCase()
    }).as[String]

//
//    upperMessageDs.foreachPartition(messageIt => {
//      //make connection to storage layer
//      // May use static connection
//      messageIt.foreach(message => {
//        //write to storage location
//        println("Got order ", message)
//      })
//    })


    val query =
      upperMessageDs.writeStream
        .format("console")
        .option("truncate","false")
        .outputMode(OutputMode.Append())

    query.start().awaitTermination()


    //messageOutput.awaitTermination()
  }
}
