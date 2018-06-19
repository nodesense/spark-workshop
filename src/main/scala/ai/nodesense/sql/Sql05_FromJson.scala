package ai.nodesense.dataframes

import ai.nodesense.util.FileUtils
import org.apache.spark.sql.SparkSession

//https://spark.apache.org/docs/latest/sql-programming-guide.html#starting-point-sparksession


object Sql05_FromJson extends  App {

  val spark = SparkSession
    .builder()
    .master("local")
    .appName("Spark SQL basic example")
    // .config("spark.some.config.option", "some-value")
    .getOrCreate()

  spark.sparkContext.setLogLevel("WARN")
  // For implicit conversions like converting RDDs to DataFrames
  import spark.implicits._


  val df = spark.read.json(FileUtils.getInputPath("persons.json"))
  df.printSchema()
  df.show()

  // now register data frame as global temporary view
  //this brings sql on top of data frame
  df.createGlobalTempView("persons")

  //df.createTempView("persons2")

  spark.sql("SELECT * FROM global_temp.persons").show()


  // Global temporary view is cross-session
  spark.newSession().sql("SELECT * FROM global_temp.persons").show()

  val under12 = spark.sql("SELECT name, age FROM global_temp.persons WHERE age BETWEEN 6 AND 12")

  under12.show()

  under12.map(name => "Name: " + under12("name")).show()

}