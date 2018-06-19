package ai.nodesense.datasets

import org.apache.spark.sql.SparkSession


object DS02_DataFrameFromSeq extends  App {

  def createDataFromSeq () = {
    val spark = SparkSession
      .builder
      .master("local")
      .appName("DS01Basics")
      .getOrCreate()

    import spark.sqlContext.implicits._

    val df = Seq(("one", 1), ("one", 1), ("two", 1))
      .toDF("word", "count")

    df.show()

    val counted = df.groupBy('word).count
    counted.show()
  }



  createDataFromSeq()
}
