package ai.nodesense.datasets


import org.apache.spark.sql.SparkSession
import ai.nodesense.models.Sales
import ai.nodesense.util.FileUtils

// Dataframe and dataset catalogs
//FIXME: Broken on refactor

object CatalogExample {

  def main(args: Array[String]) {

    val sparkSession = SparkSession.builder.
      master("local")
      .appName("example")
      .getOrCreate()


    val df = sparkSession.read
                .csv(FileUtils.getInputPath("sales.csv"))

    df.createTempView("sales")

    //interacting with catalogue

    val catalog = sparkSession.catalog

    //print the databases

    catalog.listDatabases().select("name").show()

    // print all the tables

    catalog.listTables().select("name").show()

    // is cached
    println(catalog.isCached("sales"))
    df.cache()
    println(catalog.isCached("sales"))

    // drop the table
    catalog.dropTempView("sales")
    catalog.listTables().select("name").show()

    // list functions
    catalog.listFunctions()
            .select("name","description","className","isTemporary")
            .show(100)
  }

}