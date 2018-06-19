package ai.nodesense.datasets

import org.apache.spark.sql.SparkSession
import ai.nodesense.models.Sales
import ai.nodesense.util.FileUtils
/**
  * Logical Plans for Dataframe and Dataset
  */
object DS03_DataFrameVsDataSet {


  def main(args: Array[String]) {

    val sparkSession = SparkSession.builder.
      master("local")
      .appName("example")
      .getOrCreate()

    val sparkContext = sparkSession.sparkContext
    import sparkSession.implicits._


    //read data from text file

    val df = sparkSession.read
              .option("header","true")
              .option("inferSchema","true")
              .csv(FileUtils.getInputPath("sales.csv"))

    val ds = sparkSession.read
              .option("header","true")
              .option("inferSchema","true")
              .csv(FileUtils.getInputPath("sales.csv"))
              .as[Sales]

    val selectedDF = df.select("itemId")

    val selectedDS = ds.map(_.itemId)

    println(selectedDF.queryExecution.optimizedPlan.numberedTreeString)

    println(selectedDS.queryExecution.optimizedPlan.numberedTreeString)
  }

}