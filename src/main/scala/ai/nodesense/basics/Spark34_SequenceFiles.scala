package ai.nodesense.basics


import org.apache.spark._
import org.apache.spark.SparkContext._

object SequenceFiles {
  def main(args: Array[String]) {
    val master = "local"
    val outputFile = "output/sequence-files"
    val sc = new SparkContext(master, "BasicSaveSequenceFile", System.getenv("SPARK_HOME"))
    val data = sc.parallelize(List(("Holden", 3), ("Kay", 6), ("Snail", 2)))
    data.saveAsSequenceFile(outputFile)
//
//    val data2 = sc.sequenceFile(inFile, classOf[Text], classOf[IntWritable]).map{case (x, y) =>
//      (x.toString, y.get())}
//    println(data.collect().toList)
  }
}