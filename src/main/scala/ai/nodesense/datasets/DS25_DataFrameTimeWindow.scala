package ai.nodesense.datasets

import ai.nodesense.util.FileUtils
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object DS25_DataFrameTimeWindow {


  def createSparkSession() = {
    val spark = SparkSession
      .builder
      .master("local[4]")
      .appName("DS01Basics")
      .config("spark.sql.crossJoin.enabled", "true")

      .getOrCreate()

    spark
  }


  def readStockData(spark: SparkSession, fileName:String) = {

    import spark.sqlContext.implicits._

    val stocksDF = spark.read
      .option("header", "true") //reading the headers
      .option("mode", "DROPMALFORMED")
      .option("delimiter", ",")
      .option("inferSchema", "true")
      .csv(FileUtils.getInputPath(fileName))

    stocksDF.printSchema()
    stocksDF.show(100)
    stocksDF
  }


  def printWindow(windowDF:DataFrame, aggCol:String) ={
    windowDF.sort("window.start").select("window.start","window.end",s"$aggCol").
      show(truncate = false)
  }

  def main(args: Array[String]) {

    val spark = createSparkSession()


    spark.sparkContext.setLogLevel("ERROR")

    val stocksDF = readStockData(spark, "applestock.csv")

    //weekly average of 2016

    val stocks2016 = stocksDF.filter("year(Date)==2016")

    val tumblingWindowDS = stocks2016.groupBy(window(stocks2016.col("Date"),"1 week"))
      .agg(avg("Close").as("weekly_average"))
    println("weekly average in 2016 using tumbling window is")

    printWindow(tumblingWindowDS,"weekly_average")


    val windowWithStartTime = stocks2016.groupBy(window(stocks2016.col("Date"),"1 week","1 week", "4 days")).
      agg(avg("Close").as("weekly_average"))
    println("weekly average in 2016 using sliding window is")

    printWindow(windowWithStartTime,"weekly_average")

    val filteredWindow = windowWithStartTime.filter("year(window.start)=2016")
    println("weekly average in 2016 after filtering is")

    printWindow(filteredWindow,"weekly_average")

  }
}