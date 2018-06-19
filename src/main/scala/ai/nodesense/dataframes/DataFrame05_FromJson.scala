package ai.nodesense.dataframes

import ai.nodesense.util.FileUtils
import org.apache.spark.sql.SparkSession

object DataFrame05_FromJson extends  App {

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


  df.select("name").show()
  df.select($"age").show()

  df.select($"name", $"age" + 5).show()

  df.filter($"age" < 30).show()

  // or use where

  df.where($"age" < 30).show()


  df.groupBy("age").count().show()



}