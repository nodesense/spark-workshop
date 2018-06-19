package ai.nodesense.basics

import org.apache.spark.TaskContext
import org.apache.spark.sql.SparkSession

/* Input:  No External
   Output: Console
 */

object  Spark06_KeyValueRDD extends App {

  val spark = SparkSession.builder
    .master("local[4]")
    .appName("key-value-rdd")
    .config("spark.app.id", "key-value-rdd")
    .getOrCreate()


  val sc = spark.sparkContext



  val names = List("iPhone 7", "Google Nexus", "Google Pixel", "iPhone 10", "Moto G");

  val namesRDD = sc.parallelize(names)

  namesRDD
    .map({
      name => (name.charAt(0), name)
    })
    .groupByKey()
    .mapValues( {
      names => names.size
    })
     .foreach(println(_))

  // example that show how to avoid  group by

  namesRDD
      .distinct(6)
    .map(name => (name.charAt(0), 1))
    .reduceByKey(_ + _)
    .foreach(println(_))


  println("Enter key to exit");
  scala.io.StdIn.readLine()
  sc.stop()
}
