package ai.nodesense.basics

import ai.nodesense.util.FileUtils
import org.apache.spark.SparkContext
import org.apache.spark._

object LogFiles extends  App {
  def processLogFiles(filePath: String) = {
    // load error messages from a log into memory
    // then interactively search for various patterns
    val sc = new SparkContext("local",
      "BasicLoadNums",
      System.getenv("SPARK_HOME"))


    // base RDD
    val lines = sc.textFile(filePath)

    // transformed RDDs
    val errors = lines.filter(_.contains("ERROR"))
    val warnings = lines.filter(_.contains("WARN"))
    val infos = lines.filter(_.contains("INFO"))

    println("Total Errors ", errors.count())
    println("Total Warnings ", warnings.count())
    println("Total Infos ", infos.count())
  }

  processLogFiles(FileUtils.getInputPath("zookeeper.log.txt"));

}
