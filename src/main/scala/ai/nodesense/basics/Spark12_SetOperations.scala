package ai.nodesense.basics

import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.rdd.{RDD, PairRDDFunctions}

import scala.reflect.ClassTag;

object Intersect  extends App {


  def intersectByKey[K: ClassTag, V: ClassTag](rdd1: RDD[(K, V)], rdd2: RDD[(K, V)]): RDD[(K, V)] = {
    rdd1.cogroup(rdd2).flatMapValues{
      case (Nil, _) => None
      case (_, Nil) => None
      case (x, y) => x++y
    }
  }

    val sc = new SparkContext("local", "BasicIntersectByKey", System.getenv("SPARK_HOME"))
    val rdd1 = sc.parallelize(List((1, "panda"), (2, "happy")))
    val rdd2 = sc.parallelize(List((2, "pandas")))
    val iRdd = intersectByKey(rdd1, rdd2)
    val panda: List[(Int, String)] = iRdd.collect().toList
    panda.map(println(_))
    sc.stop()

}
