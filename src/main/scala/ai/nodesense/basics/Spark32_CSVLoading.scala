package ai.nodesense.basics

import java.io.StringReader
import java.io.StringWriter

import ai.nodesense.models.Person
import ai.nodesense.util.FileUtils
import org.apache.spark._
// import play.api.libs.json._
// import play.api.libs.functional.syntax._
import scala.util.parsing.json.JSON
import scala.collection.JavaConversions._

import au.com.bytecode.opencsv.CSVReader
import au.com.bytecode.opencsv.CSVWriter

import ai.nodesense.models.Person

object CSVLoading {

  def csvLineByLine(inputFile: String, outputFile:String) = {

    val sc = new SparkContext("local", "BasicParseCsv", System.getenv("SPARK_HOME"))
    val input = sc.textFile(inputFile)


    val result = input
                .map(line => line.trim)
              .filter(line => !line.isEmpty )
              .map{ line =>
              val reader = new CSVReader(new StringReader(line));
              reader.readNext();
    }


    FileUtils.rmrf(outputFile)

    val people = result.map(x => Person(x(0), x(1).trim().toInt))
    val filteredPersons = people.filter(person => person.age <= 30)
    filteredPersons.map(person => List(person.name, person.age.toString).toArray).mapPartitions{people2 =>
      val stringWriter = new StringWriter();
      val csvWriter = new CSVWriter(stringWriter);
      csvWriter.writeAll(people2.toList)
      Iterator(stringWriter.toString)
    }.saveAsTextFile(outputFile)

  }

  def csvFileReadAll(inputFile: String) = {
    val sc = new SparkContext("local", "BasicParseWholeFileCsv", System.getenv("SPARK_HOME"))
    val input = sc.wholeTextFiles(inputFile)
    val result = input.flatMap{ case (_, txt) =>
      val reader = new CSVReader(new StringReader(txt));
      reader.readAll()
    }
    println(result.collect().map(_.toList).mkString(","))
  }

  def main(args: Array[String]) {

    val master = "local"

    csvLineByLine(FileUtils.getInputPath("persons.csv"),
      FileUtils.getOutputPath("persons-csv-results"))

    csvFileReadAll(FileUtils.getInputPath("persons.csv"))
  }
}