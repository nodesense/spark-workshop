package ai.nodesense.basics
import org.apache.spark.{SparkContext, SparkConf};

object MapPartitionAvg extends App {
  case class AvgCount(var total: Int = 0, var num: Int = 0) {
    def merge(other: AvgCount): AvgCount = {
      total += other.total
      num += other.num
      this
    }
    def merge(input: Iterator[Int]): AvgCount = {
      input.foreach{elem =>
        total += elem
        num += 1
      }
      this
    }
    def avg(): Float = {
      total / num.toFloat;
    }
  }

  def avg(filepath: String, threads: Int = 1)  = {
    val conf = new SparkConf()
      .setAppName("Spark Hello World")
      .setMaster("local") // single thread

    val sc = new SparkContext(s"local[$threads]",
                              "BasicAvgMapPartitions",
                              System.getenv("SPARK_HOME"))

    // conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")


    val input = sc.parallelize(List(1, 2, 3, 4))
    val result = input.mapPartitions(partition =>
      // Here we only want to return a single element for each partition,
      // but mapPartitions requires that we wrap our return
      // in an Iterator
      Iterator(AvgCount(0, 0).merge(partition)))
      .reduce((x,y) => x.merge(y))
    println(result)

    println("Stoping")
    sc.stop()
  }


  avg("src/main/resources/data/numbers/numbers1.txt")

}
