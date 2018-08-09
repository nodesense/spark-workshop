package ai.nodesense.ml

import ai.nodesense.util.FileUtils
import org.apache.spark.sql.SparkSession
import org.apache.spark.mllib.recommendation.ALS
import org.apache.spark.mllib.recommendation.Rating
import org.jblas.DoubleMatrix

// https://github.com/PacktPublishing/Machine-Learning-with-Spark
object ML10_MovieLens extends  App {

  val spark = SparkSession.builder()
    .appName("movie-lengs-ml")
    .master("local[8]")
    .getOrCreate()

  val sc = spark.sparkContext

  val fileRdd = sc.textFile(FileUtils.getInputPath("ml-latest-small/ratings.csv"))
  println(fileRdd.first())

  val first = fileRdd.first()

  val rawData = fileRdd.filter(line => line != first)

  // take first 3 columns, exclude timestamp
  val rawRatings = rawData.map(_.split(",").take(3))
  rawRatings.first()

  val ratings = rawRatings.map { case Array(user, movie, rating) => println("Data ", user, movie, rating)
                                        Rating(user.toInt, movie.toInt, rating.toDouble) }
  println("Ratings ", ratings.first())



  /* Train the ALS model with rank=50, iterations=10, lambda=0.01 */
  val model = ALS.train(ratings, 50, 10, 0.01)

  println("UF ", model.userFeatures)
  println("UFC ", model.userFeatures.count)
  println("PFC", model.productFeatures.count)


  /* Make a prediction for a single user and movie pair */
  //val predictedRating = model.predict(2, 39)

  //5,440
  val userId = 671
  val KK = 10
  val topKRecs = model.recommendProducts(userId, KK)
  println(topKRecs.mkString("\n"))


  /* Load movie titles to inspect the recommendations */
  val moviesFileRdd = sc.textFile(FileUtils.getInputPath("ml-latest-small/movies.csv"))
  val firstH1 = moviesFileRdd.first()


  val movies = moviesFileRdd.filter(line => line != firstH1)
  val titles = movies.map(line => line.split(",").take(2)).map(array => (array(0).toInt, array(1))).collectAsMap()

  println(titles)


  // res68: String = Frighteners, The (1996)

  val moviesForUser = ratings.keyBy(_.user).lookup(671)

  println(moviesForUser.size)

  moviesForUser.sortBy(-_.rating).take(10).map(rating => (titles(rating.product), rating.rating)).foreach(println)

  topKRecs.map(rating => (titles(rating.product), rating.rating)).foreach(println)


  /* Compute item-to-item similarities between an item and the other items */
  import org.jblas.DoubleMatrix
  val aMatrix = new DoubleMatrix(Array(1.0, 2.0, 3.0))



  /* Compute the cosine similarity between two vectors */
  def cosineSimilarity(vec1: DoubleMatrix, vec2: DoubleMatrix): Double = {
    vec1.dot(vec2) / (vec1.norm2() * vec2.norm2())
  }


  // cosineSimilarity: (vec1: org.jblas.DoubleMatrix, vec2: org.jblas.DoubleMatrix)Double
  val itemId = 567
  val itemFactor = model.productFeatures.lookup(itemId).head
  // itemFactor: Array[Double] = Array(0.15179359424040248, -0.2775955241896113, 0.9886005994661484, ...
  val itemVector = new DoubleMatrix(itemFactor)
  // itemVector: org.jblas.DoubleMatrix = [0.151794; -0.277596; 0.988601; -0.464013; 0.188061; 0.090506; ...
  cosineSimilarity(itemVector, itemVector)
  // res113: Double = 1.0000000000000002
  val sims = model.productFeatures.map{ case (id, factor) =>
    val factorVector = new DoubleMatrix(factor)
    val sim = cosineSimilarity(factorVector, itemVector)
    (id, sim)
  }
  val sortedSims = sims.top(K)(Ordering.by[(Int, Double), Double] { case (id, similarity) => similarity })
  // sortedSims: Array[(Int, Double)] = Array((567,1.0), (672,0.483244928887981), (1065,0.43267674923450905), ...
  println(sortedSims.mkString("\n"))


  /* We can check the movie title of our chosen movie and the most similar movies to it */
  println(titles(itemId))
  // Wes Craven's New Nightmare (1994)
  val sortedSims2 = sims.top(K + 1)(Ordering.by[(Int, Double), Double] { case (id, similarity) => similarity })
  sortedSims2.slice(1, 11).map{ case (id, sim) => (titles(id), sim) }.mkString("\n")


  /* Compute squared error between a predicted and actual rating */
  // We'll take the first rating for our example user 789
  val actualRating = moviesForUser.take(1)(0)
  // actualRating: Seq[org.apache.spark.mllib.recommendation.Rating] = WrappedArray(Rating(789,1012,4.0))
  val predictedRating = model.predict(671, actualRating.product)
  // ...
  // 14/04/13 13:01:15 INFO SparkContext: Job finished: lookup at MatrixFactorizationModel.scala:46, took 0.025404 s
  // predictedRating: Double = 4.001005374200248
  val squaredError = math.pow(predictedRating - actualRating.rating, 2.0)
  // squaredError: Double = 1.010777282523947E-6

  /* Compute Mean Squared Error across the dataset */
  // Below code is taken from the Apache Spark MLlib guide at: http://spark.apache.org/docs/latest/mllib-guide.html#collaborative-filtering-1
  val usersProducts = ratings.map{ case Rating(user, product, rating)  => (user, product)}
  val predictions = model.predict(usersProducts).map{
    case Rating(user, product, rating) => ((user, product), rating)
  }
  val ratingsAndPredictions = ratings.map{
    case Rating(user, product, rating) => ((user, product), rating)
  }.join(predictions)
  val MSE = ratingsAndPredictions.map{
    case ((user, product), (actual, predicted)) =>  math.pow((actual - predicted), 2)
  }.reduce(_ + _) / ratingsAndPredictions.count
  println("Mean Squared Error = " + MSE)
  // ...
  // 14/04/13 15:29:21 INFO SparkContext: Job finished: count at <console>:31, took 0.538683 s
  // Mean Squared Error = 0.08231947642632856
  val RMSE = math.sqrt(MSE)
  println("Root Mean Squared Error = " + RMSE)
  // Root Mean Squared Error = 0.28691370902473196

  /* Compute Mean Average Precision at K */

  /* Function to compute average precision given a set of actual and predicted ratings */
  // Code for this function is based on: https://github.com/benhamner/Metrics
  def avgPrecisionK(actual: Seq[Int], predicted: Seq[Int], k: Int): Double = {
    val predK = predicted.take(k)
    var score = 0.0
    var numHits = 0.0
    for ((p, i) <- predK.zipWithIndex) {
      if (actual.contains(p)) {
        numHits += 1.0
        score += numHits / (i.toDouble + 1.0)
      }
    }
    if (actual.isEmpty) {
      1.0
    } else {
      score / scala.math.min(actual.size, k).toDouble
    }
  }
  val actualMovies = moviesForUser.map(_.product)
  // actualMovies: Seq[Int] = ArrayBuffer(1012, 127, 475, 93, 1161, 286, 293, 9, 50, 294, 181, 1, 1008, 508, 284, 1017, 137, 111, 742, 248, 249, 1007, 591, 150, 276, 151, 129, 100, 741, 288, 762, 628, 124)
  val predictedMovies = topKRecs.map(_.product)
  // predictedMovies: Array[Int] = Array(27, 497, 633, 827, 602, 849, 401, 584, 1035, 1014)
  val apk10 = avgPrecisionK(actualMovies, predictedMovies, 10)
  // apk10: Double = 0.0

  /* Compute recommendations for all users */
  val itemFactors = model.productFeatures.map { case (id, factor) => factor }.collect()
  val itemMatrix = new DoubleMatrix(itemFactors)
  println(itemMatrix.rows, itemMatrix.columns)
  // (1682,50)

  // broadcast the item factor matrix
  val imBroadcast = sc.broadcast(itemMatrix)

  // compute recommendations for each user, and sort them in order of score so that the actual input
  // for the APK computation will be correct
  val allRecs = model.userFeatures.map{ case (userId, array) =>
    val userVector = new DoubleMatrix(array)
    val scores = imBroadcast.value.mmul(userVector)
    val sortedWithId = scores.data.zipWithIndex.sortBy(-_._1)
    val recommendedIds = sortedWithId.map(_._2 + 1).toSeq
    (userId, recommendedIds)
  }

  // next get all the movie ids per user, grouped by user id
  val userMovies = ratings.map{ case Rating(user, product, rating) => (user, product) }.groupBy(_._1)
  // userMovies: org.apache.spark.rdd.RDD[(Int, Seq[(Int, Int)])] = MapPartitionsRDD[277] at groupBy at <console>:21

  // finally, compute the APK for each user, and average them to find MAPK
  val K = 10
  val MAPK = allRecs.join(userMovies).map{ case (userId, (predicted, actualWithIds)) =>
    val actual = actualWithIds.map(_._2).toSeq
    avgPrecisionK(actual, predicted, K)
  }.reduce(_ + _) / allRecs.count
  println("Mean Average Precision at K = " + MAPK)
  // Mean Average Precision at K = 0.030486963254725705

  /* Using MLlib built-in metrics */

  // MSE, RMSE and MAE
  import org.apache.spark.mllib.evaluation.RegressionMetrics
  val predictedAndTrue = ratingsAndPredictions.map { case ((user, product), (actual, predicted)) => (actual, predicted) }
  val regressionMetrics = new RegressionMetrics(predictedAndTrue)
  println("Mean Squared Error = " + regressionMetrics.meanSquaredError)
  println("Root Mean Squared Error = " + regressionMetrics.rootMeanSquaredError)
  // Mean Squared Error = 0.08231947642632852
  // Root Mean Squared Error = 0.2869137090247319

  // MAPK
  import org.apache.spark.mllib.evaluation.RankingMetrics
  val predictedAndTrueForRanking = allRecs.join(userMovies).map{ case (userId, (predicted, actualWithIds)) =>
    val actual = actualWithIds.map(_._2)
    (predicted.toArray, actual.toArray)
  }
  val rankingMetrics = new RankingMetrics(predictedAndTrueForRanking)
  println("Mean Average Precision = " + rankingMetrics.meanAveragePrecision)
  // Mean Average Precision = 0.07171412913757183

  // Compare to our implementation, using K = 2000 to approximate the overall MAP
  val MAPK2000 = allRecs.join(userMovies).map{ case (userId, (predicted, actualWithIds)) =>
    val actual = actualWithIds.map(_._2).toSeq
    avgPrecisionK(actual, predicted, 2000)
  }.reduce(_ + _) / allRecs.count
  println("Mean Average Precision = " + MAPK2000)



}
