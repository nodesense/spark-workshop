package ai.nodesense.ml

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

// example inspired by https://jaceklaskowski.gitbooks.io/mastering-apache-spark/spark-mllib/spark-mllib-pipelines-example-classification.html


object ML08_TextClassificationBasic extends  App {
  val spark = SparkSession.builder()
    .appName("ml-pipe-line")
    .master("local")
    .getOrCreate()

  val sc = spark.sparkContext

  import spark.sqlContext.implicits._
  //import spark.sqlContext.implicits._


//
//  sealed trait Category
//  case object Scientific extends Category
//  case object NonScientific extends Category

  // FIXME: Define schema for Category

  case class LabeledText(id: Long, category: String, text: String)



  val data = Seq(LabeledText(0, "Scientific", "hello world"),
                 LabeledText(1, "NonScientific", "witaj swiecie")).toDF

  data.show


  case class Article(id: Long, topic: String, text: String)
  val articles = Seq(
    Article(0, "sci.math", "Hello, Math!"),
    Article(1, "alt.religion", "Hello, Religion!"),
    Article(2, "sci.physics", "Hello, Physics!"),
    Article(3, "sci.math", "Hello, Math Revised!"),
    Article(4, "sci.math", "Better Math"),
    Article(5, "alt.religion", "TGIF")).toDS

  articles.show()

  val topic2Label: Boolean => Double = isSci => if (isSci) 1 else 0
  val toLabel = udf(topic2Label)

  val labelled = articles.withColumn("label", toLabel($"topic".like("sci%"))).cache

  val Array(trainDF, testDF) = labelled.randomSplit(Array(0.75, 0.25))
  trainDF.show()
  testDF.show()

  val tokenizer = new RegexTokenizer()
    .setInputCol("text")
    .setOutputCol("words")

  val hashingTF = new HashingTF()
    .setInputCol(tokenizer.getOutputCol)  // it does not wire transformers -- it's just a column name
    .setOutputCol("features")
    .setNumFeatures(5000)


  val lr = new LogisticRegression().setMaxIter(20).setRegParam(0.01)

  val pipeline = new Pipeline().setStages(Array(tokenizer, hashingTF, lr))

  val model = pipeline.fit(trainDF)

  val trainPredictions = model.transform(trainDF)
  val testPredictions = model.transform(testDF)

  trainPredictions.select('id, 'topic, 'text, 'label, 'prediction).show()
  trainPredictions.printSchema()

  val evaluator = new BinaryClassificationEvaluator().setMetricName("areaUnderROC")

  val evaluatorParams = ParamMap(evaluator.metricName -> "areaUnderROC")
  val areaTrain = evaluator.evaluate(trainPredictions, evaluatorParams)

  println("Area Train ", areaTrain)

  val areaTest = evaluator.evaluate(testPredictions, evaluatorParams)


  println("Area Test ", areaTest)

  //Let’s tune the model’s hyperparameters (using "tools" from org.apache.spark.ml.tuning package).


  val paramGrid = new ParamGridBuilder()
    .addGrid(hashingTF.numFeatures, Array(100, 1000))
    .addGrid(lr.regParam, Array(0.05, 0.2))
    .addGrid(lr.maxIter, Array(5, 10, 15))
    .build


  // That gives all the combinations of the parameters
  println(paramGrid)


    val cv = new CrossValidator()
      .setEstimator(pipeline)
      .setEstimatorParamMaps(paramGrid)
      .setEvaluator(evaluator)
      .setNumFolds(10)

    val cvModel = cv.fit(trainDF)

  val cvPredictions = cvModel.transform(testDF)

  cvPredictions.select('topic, 'text, 'prediction).show()
  evaluator.evaluate(cvPredictions, evaluatorParams)

  val bestModel = cvModel.bestModel

  //println("Best model ", bestModel)

  cvModel.write.overwrite.save(FileUtils.getOutputPath("text-classfication-model"))

}
