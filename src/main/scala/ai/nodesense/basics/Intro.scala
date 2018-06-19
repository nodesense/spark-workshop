package ai.nodesense.basics

import org.apache.spark.sql.SparkSession

import org.apache.spark.sql.types._

object Intro extends App {

  val spark = SparkSession.builder.
    master("local[2]").
    appName("file-parse-app").
    config("spark.app.id", "file-parse-app").   // To silence Metrics warning.
    getOrCreate()

  spark.conf.set("spark.executor.cores", "2")
  spark.conf.set("spark.executor.memory", "4g")

  val sc = spark.sparkContext

  val favMovies = sc.parallelize(List("Pulp Fiction","Requiem for a dream","A clockwork Orange"));
  val result:Array[String] = favMovies.flatMap(movieTitle=>movieTitle.split(" ")).collect()

  result.
    foreach(println)

  val data = sc.parallelize(List(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20))
  val result2 = data.sample(true, 0.1, 12345).collect
  result2.foreach(println)


  // distinct
  val moviesList = sc.parallelize(List("A Nous Liberte","Airplane","The Apartment","The Apartment"))
  val result3 = moviesList.distinct().collect()
  result3.foreach(println)

  val java_skills=sc.parallelize(List("Tom Mahoney","Alicia Whitekar","Paul Jones","Rodney Marsh"))
  val db_skills= sc.parallelize(List("James Kent","Paul Jones","Tom Mahoney","Adam Waugh"))
  val result4 = java_skills.intersection(db_skills).collect()

  result4.foreach(println)

   val result5 = java_skills.union(db_skills).collect()

  result5.foreach(println)

  val result6 = java_skills.subtract(db_skills).collect()


  result6.foreach(println)

  val result7 = sc.parallelize(Seq(10, 4, 2, 12, 3))
      .takeOrdered(1)
  // returns Array(2)

  result7.foreach(println)

  val result8 = sc.parallelize(Seq(20, 2, 4, 3, 6)).takeOrdered(2)
  // returns Array(2, 3)


  result8.foreach(println)

  // Input Data
  val storeSales = sc.parallelize(Array(("London", 23.4),("Manchester",19.8),("Leeds",14.7),("London",26.6)))


  //GroupByKey
  val result9 = storeSales.groupByKey().map(location=>(location._1,location._2.sum)).collect()


  result9.foreach(println)

//  #SampleResult
//  #res2: Array[(String, Double)] = Array((Manchester,19.8), (London,50.0), (Leeds,14.7))
//
//  #ReduceByKey
  val result10 = storeSales.reduceByKey(_+_).collect();

  println("Reduce by key ")
  result10.foreach(println)

//
//  #Sample Result
//  #res1: Array[(String, Double)] = Array((Manchester,19.8), (London,50.0), (Leeds,14.7))
//
//
//

  val sampleData = sc.parallelize(Array(("k1",10),("k2",5),("k1",6),("k3",4),("k2",1),("k3",4)))
  val sumCount = sampleData.combineByKey(value => (value,1),
    (valcntpair: (Int,Int), value) => (valcntpair._1 + value, valcntpair._2+1),
    (valcntpair: (Int,Int), valcntpairnxt: (Int,Int)) => ((valcntpair._1 + valcntpairnxt._1),(valcntpair._2 + valcntpairnxt._2)))

  val result11 = sumCount.take(3)

  println("result11  ")
  result11.foreach(println)

  val avgByKey = sumCount.map{case (label,value) => (label, value._1/value._2)}

  val result12 = avgByKey.take(3)


  println("result12  ")
  result12.foreach(println)




}
