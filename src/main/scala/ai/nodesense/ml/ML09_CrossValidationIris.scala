package ai.nodesense.ml


import org.apache.spark.sql.SparkSession

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.{OneHotEncoder, StringIndexer, VectorAssembler}

import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.tuning.{ParamGridBuilder, CrossValidator}
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.sql.functions._

// Refer https://archive.ics.uci.edu/ml/datasets/iris for more


object ML09_CrossValidationIris extends  App {
  val spark = SparkSession.builder()
    .appName("ml-pipe-line")
    .master("local")
    .getOrCreate()

  val sc = spark.sparkContext

  import spark.sqlContext.implicits._

  // https://archive.ics.uci.edu/ml/machine-learning-databases/iris/iris.data
  // Data From UCI repository. Please refer to link below.
  val url = "https://archive.ics.uci.edu/ml/machine-learning-databases/iris/iris.data"
  val data = sc.parallelize(
    scala.io.Source
      .fromURL(url)
      .mkString
      .split("\n")
      .map(_.split(","))
      .map({case Array(f1,f2,f3,f4,label) => (f1.toDouble, f2.toDouble, f3.toDouble, f4.toDouble, label)})
  )
    .toDF("sepal_length", "sepal_width", "petal_length", "petal_width", "label")
    .filter($"label".isin("Iris-versicolor", "Iris-virginica"))
    .withColumn("label", when($"label" === "Iris-versicolor", 0.0).otherwise(1.0))

  val Array(training, test) = data.randomSplit(Array(0.9, 0.1), seed = 12345)

  val featureAssembler = new VectorAssembler()
    .setInputCols(Array("sepal_length", "sepal_width", "petal_length", "petal_width"))
    .setOutputCol("features")

  val lr = new LogisticRegression()
    .setMaxIter(10)

  val fullPipeline = new Pipeline().setStages(Array(featureAssembler, lr))

  val paramGrid = new ParamGridBuilder()
    .addGrid(lr.regParam, Array(0.1, 0.01))
    .build()

  val cv = new CrossValidator()
    .setEstimator(fullPipeline)
    .setEvaluator(new BinaryClassificationEvaluator)
    .setEstimatorParamMaps(paramGrid)
    .setNumFolds(5)

  val cvModel = cv.fit(training)

  cvModel.avgMetrics

  cvModel.transform(test).show()

}