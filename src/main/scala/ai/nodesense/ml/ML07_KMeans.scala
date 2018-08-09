package ai.nodesense.ml

// https://jaceklaskowski.gitbooks.io/mastering-apache-spark/spark-mllib/spark-mllib-KMeans.html
//
import ai.nodesense.util.FileUtils
import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.feature.RegexTokenizer
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.feature.HashingTF
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.tuning.ParamGridBuilder
import org.apache.spark.ml.tuning.CrossValidator
import org.apache.spark.ml.param._
import org.apache.spark.sql.functions._
import org.apache.spark.ml.feature._
import org.apache.spark.ml.clustering.KMeans


object ML07_KMeans extends  App {
  val spark = SparkSession.builder()
    .appName("ml-pipe-line")
    .master("local")
    .getOrCreate()

  val sc = spark.sparkContext

  import spark.sqlContext.implicits._

  final case class Email(id: Int, text: String)


  val emails = Seq(
    "This is an email from your lovely wife. Your mom says...",
    "SPAM SPAM spam",
    "Hello, We'd like to offer you").zipWithIndex.map(_.swap).toDF("id", "text").as[Email]



  // Prepare data for k-means
  // Pass emails through a "pipeline" of transformers
  import org.apache.spark.ml.feature._
  val tok = new RegexTokenizer()
    .setInputCol("text")
    .setOutputCol("tokens")
    .setPattern("\\W+")

  val hashTF = new HashingTF()
    .setInputCol("tokens")
    .setOutputCol("features")
    .setNumFeatures(20)

  val preprocess = (tok.transform _).andThen(hashTF.transform)

  val features = preprocess(emails.toDF)

  features.select('text, 'features).show(false)

  val kmeans = new KMeans
  val kmModel = kmeans.fit(features.toDF)

  kmModel.clusterCenters.map(_.toSparse)

  val email = Seq("hello mom").toDF("text")
  val result = kmModel.transform(preprocess(email))

  result.show(false)

}
