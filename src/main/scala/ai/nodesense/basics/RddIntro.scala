package ai.nodesense.basics

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


object RddExample {

  def main(arg: Array[String]): Unit = {

    // SPARK 1.x style, RDD is core of Spark API

    val conf = new SparkConf()
      .setAppName("RddExample")
      .setMaster("local[1]")

    val sc = new SparkContext(conf)

    val prices = List(100, 200, 300, 400, 500, 600, 700, 800, 900);

    // load initial RDD
    val rdd = sc.parallelize(prices)

    // example transformations

    rdd.filter(price => price > 500).foreach(println(_))

    //val discountedRdd = rdd.map(price => price - price * 0.20)
    val discountedRdd = rdd.map(price => {
      println("Map called with price ", price);
      (price - price * 0.20).toInt
    })

    println("Discounted")
    discountedRdd.foreach(println(_))

    println("Sum ", discountedRdd.sum())
    println("Mean ", discountedRdd.mean())
    println("Max ", discountedRdd.max())
    println("Min ", discountedRdd.min())

    println("Count ", discountedRdd.count())


    println("Collect ");
    discountedRdd.collect()
      .foreach(println(_))


    def computeAvg(input: RDD[Int]) = {
      input.aggregate((0, 0))((x, y) => (x._1 + y, x._2 + 1),
        (x,y) => (x._1 + y._1, x._2 + y._2))
    }


    val result = computeAvg(discountedRdd)
    val avg = result._1 / result._2.toFloat
    println("Result ", result)
    println("Avg ", avg)


    val foldResult = discountedRdd.fold(0)((x, y) => (x + y))

    println("Fold result ", foldResult)



    println("Enter key to exit");
    scala.io.StdIn.readLine()
    sc.stop()
  }

  }