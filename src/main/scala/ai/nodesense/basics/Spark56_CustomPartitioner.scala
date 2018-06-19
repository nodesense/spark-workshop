package ai.nodesense.basics

import org.apache.spark.Partitioner
import org.apache.spark.sql.SparkSession

object CustomPartitionerApp extends  App {


    val jsonPath = "src/main/resources/data/orders.json"
    val partitions = 4

    val sparkSession = SparkSession.builder
      .master("local")
      .appName("my-spark-app")
      .config("spark.some.config.option", "config-value")
      .getOrCreate()

    val jsonDf = sparkSession.read.json(jsonPath)

    val partitionedRdd = jsonDf.rdd.map(row => {
      val customerId = row.getAs[Long]("customerId")
      val productId = row.getAs[Long]("productId")
      val value = row.getAs[Long]("price")
      ((customerId, productId), value) //this a tuple with in a tuple
    }).repartitionAndSortWithinPartitions(new CustomPartitioner(partitions))

    val pairRdd = jsonDf.rdd.map(row => {
      val customerId = row.getAs[Long]("customerId")
      val productId = row.getAs[Long]("productId")
      val value = row.getAs[Long]("price")
      ((customerId, productId), value) //this a tuple with in a tuple
    })

    pairRdd.reduceByKey(_ + _, 100)
    pairRdd.reduceByKey(new CustomPartitioner(partitions), _ + _)


    partitionedRdd.collect().foreach(r => {
      println(r)
    })

    sparkSession.stop()
  }


class CustomPartitioner(numOfParts:Int) extends Partitioner {
  override def numPartitions: Int = numOfParts

  override def getPartition(key: Any): Int = {

    val k = key.asInstanceOf[(Long, Long)]
    val partition = Math.abs(k._1.hashCode) % numPartitions

    println(s"Key $key, Partition $partition")
    partition
  }
}
