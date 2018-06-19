package ai.nodesense.basics

import org.apache.spark.{SparkConf, SparkContext}

import org.apache.spark._
import org.apache.spark.rdd.RDD

/* the code demonstrate simple RDD map and transform
* Input: Inline Code
* Output: Console output
* */

object Spark02_MapFilter extends App {
  val conf = new SparkConf()
    .setAppName("Spark Hello World")
    .setMaster("local") // single thread

  val sc = new SparkContext(conf)

  val data = Array(0, 1, 2, 3, 4, 5, 6, 7, 8, 9)
  val distData = sc.parallelize(data)

  // Using Scala Lambda function
  val mulBy2Rdd = data.map(n => n * 2)

  // Using lazy call by Name
  val mulBy4Rdd = data.map(_ * 4)

  // Materialize RDDs, now foreach() shall call real map methods
  // we output the result in console

  mulBy2Rdd.foreach(println(_))

  mulBy4Rdd.foreach(println(_))

  println("Printing numbers divisible by 4")
  //Filter, now apply filter on mulBy2Rdd
  // filter returns true/false
  mulBy2Rdd.filter(n => n % 4 == 0)
            .foreach(println(_))

  println("Printing numbers divisible by 3")

  // call by name, lazy eval
  mulBy2Rdd.filter(_ % 3 == 0)
    .foreach(println(_))

  println("Stopping")
  sc.stop()
}



