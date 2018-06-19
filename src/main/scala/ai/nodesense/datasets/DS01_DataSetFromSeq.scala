package ai.nodesense.datasets


import ai.nodesense.util.FileUtils
import org.apache.spark.sql.SparkSession
import java.sql.Timestamp

import scala.io.StdIn
import org.apache.spark.sql.Encoders
import org.apache.spark.{SparkConf, SparkContext}

object DS01_DataSet extends  App {

  def createFromSeq() = {
    val spark = SparkSession
      .builder
      .master("local")
      .appName("DS01Basics")
      .getOrCreate()

    import spark.sqlContext.implicits._

    val ds = Seq(1, 2, 3).toDS()

    ds.printSchema()
    ds.show(100)

    ds.map(_ + 1).foreach(println(_))
  }


  createFromSeq()
}
