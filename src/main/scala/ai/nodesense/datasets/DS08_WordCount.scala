package ai.nodesense.datasets

import ai.nodesense.util.FileUtils
import org.apache.spark.sql.SparkSession

object DS02WordCount extends App  {


  def wordCount() = {
    val spark = SparkSession
      .builder
      .master("local")
      .appName("DS01Basics")
      .getOrCreate()

    import spark.sqlContext.implicits._

    val linesDS = spark.read
      .text(FileUtils.getInputPath("war-and-peace.txt"))
      .as[String]

    println("Lines Schema")
    linesDS.printSchema()

    val words = linesDS.flatMap(value => value.split("\\s+"))


    println("words Schema")
    linesDS.printSchema()

    val groupedWords = words.groupByKey(_.toLowerCase)

    // similar to reduceByKey
    val counts = groupedWords.count()

    println("CountSchema Schema")
    counts.printSchema()

    println("Dataset contents")
    counts.show()
  }

  wordCount()

}
