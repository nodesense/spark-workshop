package ai.nodesense.ml

import org.apache.spark.sql.SparkSession

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.{OneHotEncoder, StringIndexer, VectorAssembler}

object ML05_PipeLine extends  App {
  val spark = SparkSession.builder()
    .appName("ml-pipe-line")
    .master("local")
    .getOrCreate()

  val sc = spark.sparkContext

  import spark.sqlContext.implicits._

  val data = sc.parallelize(Array(
    ("M", "EN", 1.0),
    ("M", "ES", 0.0),
    ("F", "EN", 1.0),
    ("F", "ZH", 0.1)
  )).toDF("gender", "language", "label")

  // Define indexers and encoders
  val fieldsToIndex = Array("gender", "language")
  val indexers = fieldsToIndex.map(f => new StringIndexer()
    .setInputCol(f).setOutputCol(f + "_index"))

  val fieldsToEncode = Array("gender", "language")
  val oneHotEncoders = fieldsToEncode.map(f => new OneHotEncoder()
    .setInputCol(f + "_index").setOutputCol(f + "_flags"))

  val featureAssembler = new VectorAssembler()
    .setInputCols(Array("gender_flags", "language_flags"))
    .setOutputCol("features")

  // Combine stages into pipeline
  val pipeline = new Pipeline().setStages(indexers ++ oneHotEncoders :+ featureAssembler)

  pipeline
    .fit(data)
    .transform(data)
    .drop("gender_flags")
    .drop("language_flags")
    .show()

}

