package ai.nodesense.basics

import org.apache.spark.{SparkContext, SparkConf};

object FileAvg extends  App {
  def avg(filepath: String, threads: Int = 1) = {
    val conf = new SparkConf();
    conf.setMaster(s"local[$threads]");
    conf.setAppName("Avg App");

    val sc = new SparkContext(conf);

    val input = sc.textFile(filepath);

    println("All numbers")
    input.foreach(println(_));

    val result = input.map(_.toInt).aggregate((0, 0)) (
      (acc, value) => (acc._1 + value, acc._2 + 1),
      (acc1, acc2) => (acc1._1 + acc2._1, acc1._2 + acc2._2)
    )

    val avg = result._1 / result._2.toFloat;
    println("AVG ", avg);
    println("Stopping")
    sc.stop()
  }

  println("Avg from single file")
  avg("src/main/resources/data/numbers/numbers1.txt")

  println("Avg from Files");
  avg("src/main/resources/data/numbers")


}
