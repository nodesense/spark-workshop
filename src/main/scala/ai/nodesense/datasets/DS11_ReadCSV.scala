package ai.nodesense.datasets

import ai.nodesense.util.FileUtils
import org.apache.spark.sql.SparkSession
import java.sql.Timestamp

import ai.nodesense.models.{Birthdays, State}

import scala.io.StdIn
import org.apache.spark.sql.Encoders
import org.apache.spark.{SparkConf, SparkContext}

object DS11_ReadCSV extends App {

  def createSparkSession() = {
    val spark = SparkSession
      .builder
      .master("local[4]")
      .appName("DS01Basics")
      .config("spark.sql.crossJoin.enabled", "true")

      .getOrCreate()

      spark
  }

  def readStates(spark: SparkSession, fileName:String) = {
    import spark.sqlContext.implicits._

    val statesDS = spark.read
      .option("header", "true") //reading the headers
      .option("mode", "DROPMALFORMED")
      .option("delimiter", ",")
      .option("inferSchema", "true")
      .csv(FileUtils.getInputPath(fileName))
      .as[State]


    statesDS.printSchema()
    println("All states")
    statesDS.show(100)
    statesDS
  }


  val spark = createSparkSession()
  import spark.sqlContext.implicits._


  val statesDS = readStates(spark, "usa-states.csv")
  statesDS.show(100)





}
