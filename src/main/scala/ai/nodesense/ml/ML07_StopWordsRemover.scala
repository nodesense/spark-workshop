package ai.nodesense.ml

import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.feature.StopWordsRemover

object ML07_StopWordsRemover extends  App {

  val spark = SparkSession.builder()
    .appName("ml-hashing-tf")
    .master("local")
    .getOrCreate()

  val sc = spark.sparkContext
  import spark.sqlContext.implicits._
  val remover = new StopWordsRemover()
    .setInputCol("raw")
    .setOutputCol("filtered")

  val dataSet = spark.createDataFrame(Seq(
    (0, Seq("I", "saw", "the", "red", "balloon")),
    (1, Seq("Mary", "had", "a", "little", "lamb"))
  )).toDF("id", "raw")

  remover.transform(dataSet).show(false)
}
