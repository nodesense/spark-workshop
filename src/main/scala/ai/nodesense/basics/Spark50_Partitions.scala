package ai.nodesense.basics

import org.apache.spark.{HashPartitioner, RangePartitioner}
import org.apache.spark.sql.SparkSession

object  SparkPartitions  extends App {

  val spark = SparkSession.builder.
    master("local[4]").
    appName("rdd-partitions").
    config("spark.app.id", "rdd-partitions").   // To silence Metrics warning.
    getOrCreate()

  val sc = spark.sparkContext

  val prices = List(2, 4, 3, 5, 1, 2, 6, 8, 4, 9, 7)

  val pricesRDD = sc.parallelize(prices)

  println("Num partitions ", pricesRDD.getNumPartitions)
  println("partitioner ", pricesRDD.partitioner)


  var rangeRDD = sc.parallelize(List(3, 6, 9))
                  .map(value => (value, 1));


  var rangePartitioner = new RangePartitioner(5, rangeRDD)
  var hashPartitioner = new HashPartitioner(6);

  val results = pricesRDD
              //.map (x => (x, x * 10))
              .map (x => (x, x))
              //.partitionBy( new HashPartitioner(6) )
              .partitionBy(rangePartitioner)
              .glom().collect()



  println("Total partition result Size ", results.size);
  for (r <- results) {
    println("Partition's Data ", r.mkString(","))
  }

  pricesRDD
    .map (x => (x, x * 10))
    .foreachPartition( partition => {
      // Useful like here you can write code to connect to DB
      // for each partition
      println("foreachParition ", partition)
      partition.foreach(println ("P Data ", _))
    })

  //TODO REparition
  // coealsec

  println("Partitions structure: ", pricesRDD.glom().collect().mkString(","))

}
