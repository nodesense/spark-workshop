package ai.nodesense.basics

import org.apache.spark.{SparkConf, SparkContext}

import org.apache.spark._
import org.apache.spark.rdd.RDD

  /* the code demonstrate simple RDD
  * Input: Inline Code
  * Output: Console output
  *
  * */


object HelloWorld extends App {
  val conf = new SparkConf()
                .setAppName("Spark Hello World")
                .setMaster("local") // single thread

  val sc = new SparkContext(conf)

  val data = Array(1, 2, 3, 4, 5)
  val distData = sc.parallelize(data)


  // min, max, sum and count are action methods
  val min = distData.min()
  val max = distData.max()
  val sum = distData.sum();
  val count = distData.count();

  println("Min ", min);
  println("Max ", max);
  println("Sum ", sum);
  println("Count ", count);

  val result = computeAvg(distData)
  val avg = result._1 / result._2.toFloat
  println("Avg ", result)

  def computeAvg(input: RDD[Int]) = {
    input.aggregate((0, 0))((x, y) => (x._1 + y, x._2 + 1),
      (x,y) => (x._1 + y._1, x._2 + y._2))
  }

  val foldResult = distData.fold(0)((x, y) => (x + y))

  println("Fold result ", foldResult)

  println("Stopping")
  sc.stop()
}



