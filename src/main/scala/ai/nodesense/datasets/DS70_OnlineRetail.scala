package ai.nodesense.datasets

import ai.nodesense.util.FileUtils
import org.apache.spark.sql.{SparkSession, Dataset}
import java.sql.Timestamp

import ai.nodesense.models.{Order}

import scala.io.StdIn
import org.apache.spark.sql.Encoders
import org.apache.spark.{SparkConf, SparkContext}

import org.apache.spark.sql.expressions.scalalang.typed.{
  count => typedCount,
  sum => typedSum,
  avg => typedAvg
}

object DS70_OnlineRetail {


  def createSparkSession() = {
    val spark = SparkSession
      .builder
      .master("local[1]")
      .appName("DS70_OnlineRetail")
      .config("spark.sql.crossJoin.enabled", "true")

      .getOrCreate()

    spark
  }


  def readCSV(spark: SparkSession, fileName:String) = {

    import spark.sqlContext.implicits._

    val birthdaysDS = spark.read
      .option("header", "true") //reading the headers
      .option("mode", "DROPMALFORMED")
      .option("delimiter", ",")
      .option("inferSchema", "true")
      .csv(FileUtils.getInputPath(fileName))
      .as[Order]

    birthdaysDS.printSchema()
    birthdaysDS.show(100)
    birthdaysDS
  }

  def processOrders(spark: SparkSession, ordersDS:Dataset[Order]) = {
    import spark.sqlContext.implicits._

    ordersDS.printSchema()
    ordersDS.show(100)

    ordersDS.filter(order => order.invoiceNo == 536365).show(100)
  }


  def main(args: Array[String]) {

    val spark = createSparkSession()
    import spark.sqlContext.implicits._


    val birthDaysDS = readCSV(spark, "online-retail.min.csv")

    processOrders(spark, birthDaysDS)



  }

}
