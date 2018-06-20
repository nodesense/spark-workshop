package ai.nodesense.datasets

import ai.nodesense.util.{FileUtils, HdfsUtils}
import org.apache.spark.sql.{Dataset, SparkSession}
import java.sql.Timestamp

import ai.nodesense.models.{Birthdays, State}

import scala.io.StdIn
import org.apache.spark.sql.Encoders
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.expressions.scalalang.typed.{avg => typedAvg, count => typedCount, sum => typedSum}

object DS12_ProcessBirthData {


  def createSparkSession() = {
    val spark = SparkSession
      .builder
      .master("local[4]")
      .appName("DS01Basics")
      .config("spark.sql.crossJoin.enabled", "true")

      .getOrCreate()

    spark
  }


  def readBirthDays(spark: SparkSession, filePath:String) = {

    import spark.sqlContext.implicits._

    val birthdaysDS = spark.read
      .option("header", "true") //reading the headers
      .option("mode", "DROPMALFORMED")
      .option("delimiter", ",")
      .option("inferSchema", "true")
      .csv(filePath)
      .as[Birthdays]

    birthdaysDS.printSchema()
    birthdaysDS.show(100)
    birthdaysDS
  }

  def processBirthday(spark: SparkSession, birthdaysDS:Dataset[Birthdays]) = {
    import spark.sqlContext.implicits._

    birthdaysDS.filter(_.births > 100).show()

    // filter out all states whose numebr of births  exceed 1000 in the given day
    val filtered = birthdaysDS
                    .filter(bd => bd.births > 1000)
                    .map(bd => (bd.state, bd.date, bd.births))
                    .toDF("state", "date", "births")

    filtered.show(100)


    // Task: Count number of births by state, total number of records
    val byStateNameDS = birthdaysDS.groupByKey(body => body.state)
      .agg(typedCount[Birthdays](_.id).name("count(id)"),
        typedSum[Birthdays](_.births).name("sum(births)")
       )
      .withColumnRenamed("value", "group")
      .alias("Summary by states, total births")

    byStateNameDS.show(100)




    // Task: Count births by state and year

   val byStateYearBirths =  birthdaysDS.groupByKey(bd => (bd.state, bd.year))
      .agg(typedCount[Birthdays](_.id).name("count(id)"),
        typedSum[Birthdays](_.births).name("sum(births)")
      )

      .withColumnRenamed("value", "group")
      .alias("Summary by state, year, births")


    byStateYearBirths.show(100)


   }


  def main(args: Array[String]) {

    val spark = createSparkSession()
    import spark.sqlContext.implicits._


    ///data/birthdays.csv

    //FileUtils.getInputPath("birthdays.csv")


    val birthDaysDS = readBirthDays(spark, HdfsUtils.getInputPath("birthdays.csv"))

    processBirthday(spark, birthDaysDS)




  }

}
