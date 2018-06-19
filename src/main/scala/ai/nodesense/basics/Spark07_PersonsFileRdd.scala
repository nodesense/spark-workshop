package ai.nodesense.basics

// Spark 2.0 onwards, unified data model
// unified context
import org.apache.spark.sql.{SparkSession}

import ai.nodesense.util.FileUtils
import ai.nodesense.models.Person

object SparkPersons extends  App {
  val sparkSession = SparkSession
    .builder()
    .appName("persons-app")
    .master("local")
    .getOrCreate()

  val sc = sparkSession.sparkContext

  // read file
  val fileRdd = sc.textFile(FileUtils.getInputPath("persons.csv"))

  val processedRdd = fileRdd.map ( line => line.trim)
    .filter(line => !line.isEmpty)
    .map(line => {
      val tokens = line.split(",")
      Person(tokens(0), tokens(1).trim.toInt) //return person
    }) // map returns RDD[Person]

  processedRdd.cache()

  //forEach action method on rdd
  processedRdd.foreach( println(_))

  println("Person age <= 30")
  val filteredPersonResults = processedRdd.filter(person => person.age <= 30 )
    .collect()

  filteredPersonResults.foreach(println(_))

  sc.stop()
}
