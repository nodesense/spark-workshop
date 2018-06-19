package ai.nodesense

import org.apache.spark.sql.SparkSession

import scala.io.StdIn

object ExceptionHandling extends App {

  val spark = SparkSession
    .builder
      .master("local[4, 50]") // increase the count local[MAX THREAD, MAX RETRIES]
    .appName("ExceptionHandlingTest")
      //.config("spark.rpc.numRetries", 3)
    //.config("rpc.numRetries", 3)
    //  .config("spark.task.maxFailures","40") //definitely enough
    .config("spark.task.maxFailures","100") //definitely enough
    .getOrCreate()

  println("Default parallellism ", spark.sparkContext.defaultParallelism)
  val rdd = spark.sparkContext.parallelize(0 until 5);

  val results = rdd
                .map ( i => {
                    val r = math.random
                    if (r > 0.2) {
                      println(s"Throwing exception for $r")
                      throw new Exception(s"Throwing exception for $r")
                    }
                    println ("Success Random ", r)
                  (i, r)
                })

  spark.time(results.foreach { i =>
                      println("Result ", i)
                     })

  println("Press a key to stop")
  StdIn.readLine()
  spark.stop()

}
