package ai.nodesense.datasets

import ai.nodesense.util.FileUtils
import org.apache.spark.sql.SparkSession;

import ai.nodesense.models.{Customer}



object DS13_ProcessBankData extends App {

  import org.apache.spark.sql.functions.col


  val spark = SparkSession.builder.
    master("local[2]").
    //master("yarn").
    appName("file-parse-app").
    config("spark.app.id", "file-parse-app").   // To silence Metrics warning.
    getOrCreate()

  val sc = spark.sparkContext


  val df = spark.read
    .format("csv")
    .option("header", "true") //reading the headers
    .option("mode", "DROPMALFORMED")
    .option("separator", ";")
    .option("delimiter", ";")
    .option("inferSchema", "true") // Automatically infer data types

    .load(FileUtils.getInputPath("bank.csv"))
    //.load("hdfs://nodesense.local:8020/data/bank.csv")



  //df.show(100)

  df.printSchema()

  df.select("age").show()

  df.select(col("job"), col("age")).show()

  df.filter(col("age") > 50).show()

  df.groupBy(col("age")).count().show()
  //val df2 = spark.sql("SELECT * FROM csv. `src/main/resources/data/bank.csv`")

  // Register the DataFrame as a SQL temporary view
  df.createOrReplaceTempView("customers")


  val sqlDF = spark.sql("SELECT age, marital FROM customers")
  sqlDF.show()

  // Register the DataFrame as a global temporary view
  // Global temporary view is tied to a system preserved database `global_temp`

  df.createGlobalTempView("people")
  spark.sql("SELECT age, education FROM global_temp.people").show()


  // Global temporary view is cross-session
  //spark.newSession().sql("SELECT age, education FROM global_temp.people").show()

  //df2.show()


  //private val customerEncoder = Seq(Customer(0, "", "", "", "", 0, "","","", 0, "", 0, 0, 0, 0, "", "")).toDS

  val csvDS2 = spark.read
    .format("csv")
    .option("header", "true") //reading the headers
    .option("mode", "DROPMALFORMED")
    .option("separator", ";") // seems like delimiter working not separator
    .option("delimiter", ";")
    .option("inferSchema", "true") // Automatically infer data types
    .csv("src/main/resources/data/bank.csv")
    //.load("src/main/resources/data/bank.csv")

  // import csvDS2.sparkSession.implicits._

  //val customerDS = csvDS2.as[Customer]
  //customerDS.show()

}
