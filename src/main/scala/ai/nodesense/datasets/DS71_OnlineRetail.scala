package ai.nodesense.datasets

import ai.nodesense.models.Order
import ai.nodesense.util.FileUtils
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.functions.{count, countDistinct, max, sum, asc, col}

object DS71_OnlineRetail {


  def createSparkSession() = {
    val spark = SparkSession
      .builder
      .master("local[4]")
      .appName("DS70_OnlineRetail")
      .config("spark.sql.crossJoin.enabled", "true")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

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
    // birthdaysDS.show(100)
    birthdaysDS.cache()
    birthdaysDS
  }

  def processOrders(spark: SparkSession, ordersDS:Dataset[Order]) = {

    val result  = ordersDS.collect();
  }

  def distinctCountriesCountDS(spark: SparkSession, ordersDS:Dataset[Order]) = {
      val distinctDf = ordersDS.agg(countDistinct("country"))
      val result =  distinctDf.collect()

        ordersDS.distinct()

      result.foreach(row => println(row));
      println("distinctCountriesCountDS ", result.length)

  }


  def distinctCountriesCountDSWithPartition(spark: SparkSession, ordersDS:Dataset[Order]) = {
    import spark.sqlContext.implicits._

    val distinctDf = ordersDS
                    .repartition(20, 'country)
                    .agg(countDistinct("country"))
    val result =  distinctDf.collect()
    println("distinctCountriesCountDSWithPartition ", result.length)
  }


  def distinctCountriesDS(spark: SparkSession, ordersDS:Dataset[Order]) = {
    import spark.sqlContext.implicits._

    val distinctDf = ordersDS
        .select("country")
        .distinct()

    val result =  distinctDf.collect()
    println("distinctCountriesDS ", result.length)
  }

  def distinctQueryDump(spark: SparkSession,
                        ordersDS:Dataset[Order],
                        columnName:String,
                        filePath: String
                       ) = {
    import spark.sqlContext.implicits._

    val distinctDf = ordersDS
      .select(columnName, "Description", "UnitPrice")
      .repartition(20, col(columnName))
      .distinct()
      .orderBy(asc(columnName))

    val result =  distinctDf.collect()
    println("distinct  ",columnName,  result.length)

      distinctDf
        .repartition(1)
        .write.format("com.databricks.spark.csv")
        .option("header", "true")
        .save(filePath)
  }

  def distinctCountriesDSWithPartition(spark: SparkSession, ordersDS:Dataset[Order]) = {
    import spark.sqlContext.implicits._

    val distinctDf = ordersDS
        .select("country")
        .repartition(20, 'country)
        .distinct()

    val result =  distinctDf.collect()
    println("distinctCountriesDSWithPartition ", result.length)
  }

  def amountAndQuantityByCountry(spark: SparkSession, ordersDS:Dataset[Order]) = {
    import spark.sqlContext.implicits._
    val result =   ordersDS
      .groupBy("country")
        .agg(
          sum($"quantity") as "totalItems",
          sum($"quantity" * $"unitPrice") as "amount"
        )
      .orderBy(asc("amount"))


    println(result.first());
  }


  def amountAndQuantityByCountryWithPartition(spark: SparkSession, ordersDS:Dataset[Order]) = {
    import spark.sqlContext.implicits._
    val result =   ordersDS
        .repartition(4, 'country)
      .groupBy("country")
      .agg(
        sum($"quantity") as "totalItems",
        sum($"quantity" * $"unitPrice") as "amount"
      )
      .orderBy(asc("amount"))

    println(result.first());
  }


  def time[R](description:String, block: => R): R = {
    val t0 = System.currentTimeMillis()
    val result = block    // call-by-name
    val t1 = System.currentTimeMillis()
    println(description, " Elapsed time: " + (t1 - t0) + "ms")
    result
  }

  def main(args: Array[String]) {

    val spark = createSparkSession()

    val birthDaysDS = readCSV(spark, "online-retail.csv")

    time("Process Data", processOrders(spark, birthDaysDS))

   // time("distinctCountriesDS", distinctCountriesCountDS(spark, birthDaysDS))


    //time("distinctCountriesDSWithPartition", distinctCountriesCountDSWithPartition(spark, birthDaysDS))

    //time("distinctCountriesDS WithPartition", distinctCountriesDSWithPartition(spark, birthDaysDS))


    //time("distinctCountriesDS without partition", distinctCountriesDS(spark, birthDaysDS))


   // distinctQueryDump(spark, birthDaysDS, "CustomerID", FileUtils.getOutputPath("CustomerID.csv"))
    distinctQueryDump(spark, birthDaysDS, "StockCode", FileUtils.getOutputPath("StockCode.csv"))
    //distinctQueryDump(spark, birthDaysDS, "InvoiceNo", FileUtils.getOutputPath("InvoiceNo.csv"))


    //time("amountAndQuantityByCountry without partition", amountAndQuantityByCountry(spark, birthDaysDS))

    //time("amountAndQuantityByCountryWithPartition  ", amountAndQuantityByCountryWithPartition(spark, birthDaysDS))



  }

}
