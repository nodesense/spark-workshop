package ai.nodesense.ml

import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.feature.{CountVectorizer, CountVectorizerModel}

//CountVectorizer and CountVectorizerModel aim to help convert a
// collection of text documents to vectors of token counts

object ML06_CountVectorizer extends App  {


  val spark = SparkSession.builder()
    .appName("ml-hashing-tf")
    .master("local")
    .getOrCreate()

  val sc = spark.sparkContext

  val df = spark.createDataFrame(Seq(
    (0, Array("a", "b", "c")),
    (1, Array("a", "b", "b", "c", "a"))
  )).toDF("id", "words")

  // fit a CountVectorizerModel from the corpus
  val cvModel: CountVectorizerModel = new CountVectorizer()
    .setInputCol("words")
    .setOutputCol("features")
    .setVocabSize(3)
    .setMinDF(2)
    .fit(df)

  // alternatively, define CountVectorizerModel with a-priori vocabulary
  // a-priori means a language doesn't have vocabulary of their own
  val cvm = new CountVectorizerModel(Array("a", "b", "c"))
    .setInputCol("words")
    .setOutputCol("features")

  cvModel.transform(df).show(false)
}
