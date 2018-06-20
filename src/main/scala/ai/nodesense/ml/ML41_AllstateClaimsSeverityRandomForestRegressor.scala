package ai.nodesense.ml


import ai.nodesense.util.FileUtils
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.regression.{RandomForestRegressionModel, RandomForestRegressor}
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.tuning.ParamGridBuilder
import org.apache.spark.ml.tuning.CrossValidator
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.mllib.evaluation.RegressionMetrics

//import scopt.OptionParser

import org.apache.log4j.LogManager

/**
  * Simple and silly solution for the "Allstate Claims Severity" competition on Kaggle
  * Competition page: https://www.kaggle.com/c/allstate-claims-severity
  */
object ML41_AllstateClaimsSeverityRandomForestRegressor {

  /*
   * case class for parsing command line params
   */

  case class Params(s3AccessKey: String = "", s3SecretKey: String = "",

                   // trainInput: String = "",
                  //  testInput: String = "",
                  //  outputFile: String = "",

                    trainInput: String = FileUtils.getInputPath("allstate/train.csv"),
                    testInput: String = FileUtils.getInputPath("allstate/test.csv"),


                    outputFile: String = FileUtils.getOutputPath("allstate-random-forest"),


                    algoNumTrees: Seq[Int] = Seq(3),
                    algoMaxDepth: Seq[Int] = Seq(4),
                    algoMaxBins: Seq[Int] = Seq(32),
                    numFolds: Int = 10,
                    trainSample: Double = 1.0,
                    testSample: Double = 1.0)

  /*
   * Computation logic
   */
  def process(params: Params) {

    /*
     * Initializing Spark session and logging
     */

    val sparkSession = SparkSession.builder.
      appName("AllstateClaimsSeverityRandomForestRegressor")
      .master("local[16]")
      .getOrCreate()

    import sparkSession.implicits._

    val log = LogManager.getRootLogger


    // ****************************
    log.info("Loading input data")
    // ****************************

    if (params.trainInput.startsWith("s3://")) {
      sparkSession.conf.set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
      sparkSession.conf.set("spark.hadoop.fs.s3a.access.key", params.s3AccessKey)
      sparkSession.conf.set("spark.hadoop.fs.s3a.secret.key", params.s3SecretKey)
    }

    // *************************************************
    log.info("Reading data from train.csv file")
    // *************************************************

    val trainInput = sparkSession.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(params.trainInput)
      .cache

    val testInput = sparkSession.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(params.testInput)
      .cache

    // *******************************************
    log.info("Preparing data for training model")
    // *******************************************

    val data = trainInput.withColumnRenamed("loss", "label")
      .sample(false, params.trainSample)

    val splits = data.randomSplit(Array(0.7, 0.3))
    val (trainingData, validationData) = (splits(0), splits(1))

    trainingData.cache
    validationData.cache

    val testData = testInput.sample(false, params.testSample).cache

    // **************************************************
    log.info("Building Machine Learning pipeline")
    // **************************************************

    // StringIndexer for categorical columns (OneHotEncoder should be evaluated as well)
    def isCateg(c: String): Boolean = c.startsWith("cat")
    def categNewCol(c: String): String = if (isCateg(c)) s"idx_${c}" else c

    val stringIndexerStages = trainingData.columns.filter(isCateg)
      .map(c => new StringIndexer()
        .setInputCol(c)
        .setOutputCol(categNewCol(c))
        .setHandleInvalid("skip") //by Krish
        .fit(trainInput.select(c).union(testInput.select(c))))

    // Function to remove categorical columns with too many categories
    def removeTooManyCategs(c: String): Boolean = !(c matches "cat(109$|110$|112$|113$|116$)")

    // Function to select only feature columns (omit id and label)
    def onlyFeatureCols(c: String): Boolean = !(c matches "id|label")

    // Definitive set of feature columns
    val featureCols = trainingData.columns
      .filter(removeTooManyCategs)
      .filter(onlyFeatureCols)
      .map(categNewCol)

    // VectorAssembler for training features
    val assembler = new VectorAssembler()
      .setInputCols(featureCols)
      .setOutputCol("features")

    // Estimator algorithm
    val algo = new RandomForestRegressor().setFeaturesCol("features").setLabelCol("label")

    // Building the Pipeline for transformations and predictor
    val pipeline = new Pipeline().setStages((stringIndexerStages :+ assembler) :+ algo)


    // ***********************************************************
    log.info("Preparing K-fold Cross Validation and Grid Search")
    // ***********************************************************

    val paramGrid = new ParamGridBuilder()
      .addGrid(algo.numTrees, params.algoNumTrees)
      .addGrid(algo.maxDepth, params.algoMaxDepth)
      .addGrid(algo.maxBins, params.algoMaxBins)
      .build()

    val cv = new CrossValidator()
      .setEstimator(pipeline)
      .setEvaluator(new RegressionEvaluator)
      .setEstimatorParamMaps(paramGrid)
      .setNumFolds(params.numFolds)


    // ************************************************************
    log.info("Training model with RandomForest algorithm")
    // ************************************************************

    val cvModel = cv.fit(trainingData)


    // **********************************************************************
    log.info("Evaluating model on train and test data and calculating RMSE")
    // **********************************************************************

    val trainPredictionsAndLabels = cvModel.transform(trainingData).select("label", "prediction")
      .map { case Row(label: Double, prediction: Double) => (label, prediction) }.rdd

    val validPredictionsAndLabels = cvModel.transform(validationData).select("label", "prediction")
      .map { case Row(label: Double, prediction: Double) => (label, prediction) }.rdd

    val trainRegressionMetrics = new RegressionMetrics(trainPredictionsAndLabels)
    val validRegressionMetrics = new RegressionMetrics(validPredictionsAndLabels)

    val bestModel = cvModel.bestModel.asInstanceOf[PipelineModel]
    val featureImportances = bestModel.stages.last.asInstanceOf[RandomForestRegressionModel].featureImportances.toArray

    val output = "\n=====================================================================\n" +
      s"Param trainSample: ${params.trainSample}\n" +
      s"Param testSample: ${params.testSample}\n" +
      s"TrainingData count: ${trainingData.count}\n" +
      s"ValidationData count: ${validationData.count}\n" +
      s"TestData count: ${testData.count}\n" +
      "=====================================================================\n" +
      s"Param algoNumTrees = ${params.algoNumTrees.mkString(",")}\n" +
      s"Param algoMaxDepth = ${params.algoMaxDepth.mkString(",")}\n" +
      s"Param algoMaxBins = ${params.algoMaxBins.mkString(",")}\n" +
      s"Param numFolds = ${params.numFolds}\n" +
      "=====================================================================\n" +
      s"Training data MSE = ${trainRegressionMetrics.meanSquaredError}\n" +
      s"Training data RMSE = ${trainRegressionMetrics.rootMeanSquaredError}\n" +
      s"Training data R-squared = ${trainRegressionMetrics.r2}\n" +
      s"Training data MAE = ${trainRegressionMetrics.meanAbsoluteError}\n" +
      s"Training data Explained variance = ${trainRegressionMetrics.explainedVariance}\n" +
      "=====================================================================\n" +
      s"Validation data MSE = ${validRegressionMetrics.meanSquaredError}\n" +
      s"Validation data RMSE = ${validRegressionMetrics.rootMeanSquaredError}\n" +
      s"Validation data R-squared = ${validRegressionMetrics.r2}\n" +
      s"Validation data MAE = ${validRegressionMetrics.meanAbsoluteError}\n" +
      s"Validation data Explained variance = ${validRegressionMetrics.explainedVariance}\n" +
      "=====================================================================\n" +
      //  s"CV params explained: ${cvModel.explainParams}\n" +
      //  s"RandomForest params explained: ${bestModel.stages.last.asInstanceOf[RandomForestRegressionModel].explainParams}\n" +
      s"RandomForest features importances:\n ${featureCols.zip(featureImportances).map(t => s"\t${t._1} = ${t._2}").mkString("\n")}\n" +
      "=====================================================================\n"

    log.info(output)


    // *****************************************
    log.info("Run prediction over test dataset")
    // *****************************************

    // Predicts and saves file ready for Kaggle!
    if(!params.outputFile.isEmpty){
      cvModel.transform(testData)
        .select("id", "prediction")
        .withColumnRenamed("prediction", "loss")
        .coalesce(1)
        .write.format("csv")
        .option("header", "true")
        .save(params.outputFile)
    }
  }


  /*
   * entry point - main method
   */
  def main(args: Array[String]) {

    /*
     * Reading command line parameters
     */
//
//    val parser = new OptionParser[Params]("AllstateClaimsSeverityRandomForestRegressor") {
//      head("AllstateClaimsSeverityRandomForestRegressor", "1.0")
//
//      opt[String]("s3AccessKey").required().action((x, c) =>
//        c.copy(s3AccessKey = x)).text("The access key for S3")
//
//      opt[String]("s3SecretKey").required().action((x, c) =>
//        c.copy(s3SecretKey = x)).text("The secret key for S3")
//
//      opt[String]("trainInput").required().valueName("<file>").action((x, c) =>
//        c.copy(trainInput = x)).text("Path to file/directory for training data")
//
//      opt[String]("testInput").required().valueName("<file>").action((x, c) =>
//        c.copy(testInput = x)).text("Path to file/directory for test data")
//
//      opt[String]("outputFile").valueName("<file>").action((x, c) =>
//        c.copy(outputFile = x)).text("Path to output file")
//
//      opt[Seq[Int]]("algoNumTrees").valueName("<n1[,n2,n3...]>").action((x, c) =>
//        c.copy(algoNumTrees = x)).text("One or more options for number of trees for RandomForest model. Default: 3")
//
//      opt[Seq[Int]]("algoMaxDepth").valueName("<n1[,n2,n3...]>").action((x, c) =>
//        c.copy(algoMaxDepth = x)).text("One or more values for depth limit. Default: 4")
//
//      opt[Seq[Int]]("algoMaxBins").valueName("<n1[,n2,n3...]>").action((x, c) =>
//        c.copy(algoMaxBins = x)).text("One or more values for depth limit. Default: 32")
//
//      opt[Int]("numFolds").action((x, c) =>
//        c.copy(numFolds = x)).text("Number of folds for K-fold Cross Validation. Default: 10")
//
//      opt[Double]("trainSample").action((x, c) =>
//        c.copy(trainSample = x)).text("Sample fraction from 0.0 to 1.0 for train data")
//
//      opt[Double]("testSample").action((x, c) =>
//        c.copy(testSample = x)).text("Sample fraction from 0.0 to 1.0 for test data")
//
//    }
//
//    parser.parse(args, Params()) match {
//      case Some(params) =>
//        process(params)
//      case None =>
//        throw new IllegalArgumentException("One or more parameters are invalid or missing")
//    }


    process(Params())
  }
}