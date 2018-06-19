package ai.nodesense.datasets

import org.apache.spark.sql.SparkSession
import ai.nodesense.models.Person

object DS04_DataFrameFromCase extends  App {


  def createDataFromCaseClass () = {
    val spark = SparkSession
      .builder
      .master("local")
      .appName("DS01Basics")
      .getOrCreate()

    val people = Seq(Person("Jacek", 42),
      Person("Patryk", 19),
      Person("Maksym", 5),
      Person("Venkat", 19))

    val df = spark.createDataFrame(people)

    df.printSchema()

    df.show()

    val counted = df.groupBy("age").count
    counted.show()
  }


  createDataFromCaseClass()
}
