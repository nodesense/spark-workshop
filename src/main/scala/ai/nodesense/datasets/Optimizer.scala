package ai.nodesense.datasets

import ai.nodesense.models.Person

import ai.nodesense.util.FileUtils
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.functions.{count, countDistinct, max, avg, sum, asc, col}

object Optimizer extends  App {

  val spark = SparkSession
    .builder
    .master("local[4]")
    .appName("DS70_OnlineRetail")
    .config("spark.sql.crossJoin.enabled", "true")
    .getOrCreate()

  spark.sparkContext.setLogLevel("WARN")
  import spark.sqlContext.implicits._

  val personDS = List(
                  Person("John", 28),
                  Person("John", 34),
                  Person("May", 45),
                  Person("Unknown", 36),
                  Person("July", -1),
                  Person("Krish", 35),
                  Person("Nila", 20)
                  ).toDS()

  //personDS.explain(true)


  val filteredRDD = personDS
                    .orderBy(asc("name"))
                    .filter(col("name").isNotNull)
                    .filter(col("age").isNotNull)
                    .filter(col("name") > "A")
                    .groupBy("name")
                    .agg(avg("age"))


  filteredRDD.explain(true)

  filteredRDD.show()

}
