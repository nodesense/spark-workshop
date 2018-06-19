package ai.nodesense.basics

import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}


object RddCacheExample {

  def main(arg: Array[String]): Unit = {

    // SPARK 1.x style, RDD is core of Spark API

    val conf = new SparkConf()
      .setAppName("RddCache")
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
      price - price * 0.20
    })

    // cache() calls persist, store data in memory ie RAM
    discountedRdd.cache()
    // cache() or persist() are same here
    //discountedRdd.persist()

    // This shall store to hard disk rather than memory
   discountedRdd.persist(StorageLevel.DISK_ONLY)

    // others: Store deserialized object
    //MEMORY_ONLY_SER Apply Java Serialization
    //MEMORY_ONLY_2, MEMORY_AND_DISK_2 replicates each partition to two cluster node


    println("Discounted")
    discountedRdd.foreach(println(_))

    println("Sum ", discountedRdd.sum())
    println("Avg ", discountedRdd.mean())
    println("Max ", discountedRdd.max())
    println("Min ", discountedRdd.min())

    println("Count ", discountedRdd.count())


    println("Collect ");
    discountedRdd.collect()
      .foreach(println(_))
  }

}