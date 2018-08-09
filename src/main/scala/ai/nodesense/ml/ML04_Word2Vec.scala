package ai.nodesense.ml

import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.feature.Word2Vec
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.sql.Row


//Word2Vec is an Estimator which takes sequences of words representing documents and trains a Word2VecModel.
// The model maps each word to a unique fixed-size vector.
// Use Case: Document similarity prediction

object ML04_Word2Vec extends  App {

  val spark = SparkSession.builder()
    .appName("ml-hashing-tf")
    .master("local")
    .getOrCreate()

  val sc = spark.sparkContext
  import spark.sqlContext.implicits._

  // Input data: Each row is a bag of words from a sentence or document.
  val documentDF = spark.createDataFrame(Seq(
    "Hi I heard about Spark".split(" "),
    "I wish Java could use case classes".split(" "),
    "Logistic regression models are neat".split(" ")
  ).map(Tuple1.apply)).toDF("text")

  // Learn a mapping from words to Vectors.
  val word2Vec = new Word2Vec()
    .setInputCol("text")
    .setOutputCol("result")
    .setVectorSize(3)
    .setMinCount(0)
  val model = word2Vec.fit(documentDF)

  val result = model.transform(documentDF)
  result.collect().foreach { case Row(text: Seq[_], features: Vector) =>
    println(s"Text: [${text.mkString(", ")}] => \nVector: $features\n") }
}
