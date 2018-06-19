package ai.nodesense.datasets

import ai.nodesense.models.Person
import org.apache.spark.sql.SparkSession


object DS02_DataSetFromCaseClass extends  App {


  def createDataFromCaseClass () = {
    val spark = SparkSession
      .builder
      .master("local")
      .appName("DS01Basics")
      .getOrCreate()

    val people = Seq(Person("Karthi", 42),
      Person("Bala", 19),
      Person("Suresh", 5),
      Person("Venkat", 19))
    val df = spark.createDataFrame(people)

    df.printSchema()

    df.show()

    val counted = df.groupBy("age").count
    counted.show()
  }


  createDataFromCaseClass()
}
