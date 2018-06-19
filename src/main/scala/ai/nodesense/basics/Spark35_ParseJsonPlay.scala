package ai.nodesense.basics

import ai.nodesense.models.Person
import ai.nodesense.util.FileUtils
import org.apache.spark._
import play.api.libs.json._
import play.api.libs.functional.syntax._

object BasicParseJson {
   implicit val personReads = Json.format[Person]

  def main(args: Array[String]) {

    val master = "local"
    val inputFile = FileUtils.getInputPath("persons.json")
    val outputFile = FileUtils.getOutputPath("persons-json-results")
    val sc = new SparkContext(master, "BasicParseJson", System.getenv("SPARK_HOME"))
    val input = sc.textFile(inputFile)
    val parsed = input.map(Json.parse(_))
    // We use asOpt combined with flatMap so that if it fails to parse we
    // get back a None and the flatMap essentially skips the result.
    val result = parsed.flatMap(record => personReads.reads(record).asOpt)
    result.filter(person => person.age <= 30).map(Json.toJson(_)).saveAsTextFile(outputFile)
  }
}