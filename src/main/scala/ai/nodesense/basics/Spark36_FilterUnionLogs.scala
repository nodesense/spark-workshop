package ai.nodesense.basics

import org.apache.spark.{SparkContext, SparkConf};

object FilterUnionLogs extends  App {
  def avg(filepath: String, threads: Int = 1) = {
    val conf = new SparkConf();
    conf.setMaster(s"local[$threads]");
    conf.setAppName("Avg App");

    val sc = new SparkContext(conf);

    val inputRDD = sc.textFile(filepath)

    println("All numbers")
    inputRDD.foreach(println(_))

    val errorsRDD = inputRDD.filter(_.contains("error"))
    val warningsRDD = inputRDD.filter(_.contains("warn"))
    val badLinesRDD = errorsRDD.union(warningsRDD)

    val infosRDD = inputRDD.filter(_.contains("info"))
    val debugRDD = inputRDD.filter(_.contains("debug"))

    val notBadLinesRDD = infosRDD.union(debugRDD)
    println(badLinesRDD.collect().mkString("\n"))

    println("All lines ", inputRDD.count())
    println("badLines ", badLinesRDD.count())
    println("Not badLines ", notBadLinesRDD.count())

    println("Stopping")
    sc.stop()
  }

  println("Avg from single file")
  avg("src/main/resources/logs/application.log")



}
