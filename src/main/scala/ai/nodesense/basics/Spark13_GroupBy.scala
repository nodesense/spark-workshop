package ai.nodesense.basics

import scala.util.Random
import org.apache.spark.sql.SparkSession

object GroupBy extends App {

  def groupByExample() = {
    val spark = SparkSession
      .builder
      .appName("GroupBy Test")
      .master("local")
      .getOrCreate()

    val numMappers =  2
    val numKVPairs = 1000
    val valSize =   1000
    val numReducers = numMappers

    val rdd = spark.sparkContext.parallelize(0 until numMappers, numMappers);


    val pairs1 = rdd.flatMap { p =>
      val ranGen = new Random
      val arr1 = new Array[(Int, Array[Byte])](numKVPairs)
      for (i <- 0 until numKVPairs) {
        val byteArr = new Array[Byte](valSize)
        ranGen.nextBytes(byteArr)
        arr1(i) = (ranGen.nextInt(Int.MaxValue), byteArr)
      }
      arr1
    }.cache()
    // Enforce that everything has been calculated and in cache
    pairs1.count()

    println(pairs1.groupByKey(numReducers).count())

    spark.stop()
  }

  groupByExample()

}
