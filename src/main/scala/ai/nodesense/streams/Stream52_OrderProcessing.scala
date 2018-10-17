package ai.nodesense.streams

import java.io.{FileWriter, File}

import ai.nodesense.models.Order
import ai.nodesense.util.FileUtils
import org.apache.spark.sql.{ForeachWriter, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.expressions.scalalang.typed.{avg => typedAvg, count => typedCount, sum => typedSum}

import java.util.Properties


object Stream52_OrderStream {
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


    sparkSession.sparkContext.setLogLevel("WARN")


    import sparkSession.implicits._

    val socketLines = sparkSession.readStream
      .format("socket")
      .option("host", host)
      .option("port", port)
    //  .option("checkpointLocation", checkpointDir)
      .load()

    val ordersDs = socketLines
                     .as[String]
                    .map (line => line.trim)
                    .filter(line => !line.isEmpty)
                    .map(line => {
                      Order.buildFromLine(line)
                     })
                    .filter(order => order != null)

                    .as[Order]



//    val totalPriceDS = ordersDs.groupByKey(order => order.invoiceNo)
//      .agg(typedCount[Order](_.invoiceNo).name("count(orders)"),
//        typedSum[Order]( order => order.quantity * order.unitPrice ).name("sum(price)")
//      )
//      .withColumnRenamed("value", "invoiceNo")
//      .alias("Summary by invoiceNo, price, qty total")


    val totalPriceDS = ordersDs.withWatermark("invoiceDate", "10 seconds")
                                .groupBy("invoiceNo")
                                .agg(
                                      max($"customerId") as "customerId",
                                      count($"invoiceNo") as "totalItems",
                                      sum($"quantity" * $"unitPrice") as "amount",
                                      max($"invoiceDate") as "invoiceDate"
                                      )
    //.orderBy("invoiceDate")


    val query =
      totalPriceDS.writeStream
        .format("console")
        .option("truncate","false")
        .outputMode(OutputMode.Complete())


    val writerForText = new ForeachWriter[Row] {
     // var fileWriter: FileWriter = _

      override def process(value: Row): Unit = {
        //fileWriter.append(value.toSeq.mkString(","))
        println(value.toSeq.mkString(","))
       // fileWriter.append("\r\n")
      }

      override def close(errorOrNull: Throwable): Unit = {
       // fileWriter.close()
      }

      override def open(partitionId: Long, version: Long): Boolean = {
        //FileUtils.forceMkdir(new File(s"src/test/resources/${partitionId}"))
       // FileUtils.mkdir(FileUtils.getOutputPath(s"orders/${partitionId}"))
        //fileWriter = new FileWriter(new File(FileUtils.getOutputPath(s"orders/${partitionId}/results.csv")))
        true

      }
    }

    var writeToCSVQuery = totalPriceDS.
                          writeStream
                    //      .format("parquet")        // can be "orc", "json", "csv", etc.
                          .option("checkpointLocation", FileUtils.getOutputDirPath)
                      //     .option("path", FileUtils.getOutputPath("orders-output.parquet"))
                            .outputMode(OutputMode.Update())
                          .foreach(writerForText)


    query.start()
    writeToCSVQuery.start().awaitTermination()




    query.start().awaitTermination()
    //messageOutput.awaitTermination()
  }
}
