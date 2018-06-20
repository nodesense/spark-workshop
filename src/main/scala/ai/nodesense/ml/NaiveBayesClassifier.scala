package ai.nodesense.ml

import ai.nodesense.util.FileUtils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.classification.NaiveBayes
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator

/**

  * NaiveBayes  classification for pen digit data set: datasource:http://archive.ics.uci.edu/ml/

  */
object ML30_NaiveBayesClassifier_PenDigits extends  App {

    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkSession
      .builder
      .master("local[*]")
      .appName("naive-bayes_classification")
      .getOrCreate()

    val training  = spark.read
      .option("header", false).option("delimiter", " ")
      .format("libsvm").load(FileUtils.getInputPath("pendigits"))
    val testing  = spark.read
      .option("header", false).option("delimiter", " ")
      .format("libsvm").load(FileUtils.getInputPath("pendigits.t"))
    training.take(5).foreach(println)


    val model = new NaiveBayes()
      .fit(training)

    // Select example rows to display.
    val predictions = model.transform(testing)
    predictions.show()

    // Select (prediction, true label) and compute test error
    val evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")
      .setMetricName("accuracy")
    val accuracy = evaluator.evaluate(predictions)
    println("Accuracy: " + accuracy)


}