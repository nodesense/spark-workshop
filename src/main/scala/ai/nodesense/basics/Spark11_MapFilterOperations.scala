package ai.nodesense.basics

import org.apache.spark._

object MapOperations extends App {

  def mapWithoutCache() = {
    val sc = new SparkContext("local", "BasicMapNoCache", System.getenv("SPARK_HOME"))
    val input = sc.parallelize(List(1,2,3,4))
    val result = input.map(x => {
      println("Hi ", x);
      x*x
    })

    // map is resolved for every iteration
    result.foreach(println("XX", _));

    // will compute result twice
    println(result.count())
    println(result.collect().mkString(","))
    sc.stop()
  }

  def mapWithFilter() = {
    val sc = new SparkContext("local", "BasicMap", System.getenv("SPARK_HOME"))
    val input = sc.parallelize(List(1,2,3,4))
    val squared = input.map(x => x*x)
    val result = squared.filter(x => x != 1)
    println(result.collect().mkString(","))
    sc.stop()
  }

  mapWithoutCache()
  mapWithFilter()
}
