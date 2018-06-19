package ai.nodesense.datasets

import ai.nodesense.models.CancerClass
import ai.nodesense.util.FileUtils
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

object  DS40_CancerDataProcessingDF extends  App {
  val recordSchema = new StructType()
    .add("sample", "long")
    .add("cThick", "integer")
    .add("uCSize", "integer")
    .add("uCShape", "integer")
    .add("mAdhes", "integer")
    .add("sECSize", "integer")
    .add("bNuc", "integer")
    .add("bChrom", "integer")
    .add("nNuc", "integer")
    .add("mitosis", "integer")
    .add("clas", "integer")

  def loadCSVAsDataFrame(filePath: String) = {
    val spark = SparkSession.builder.
      master("local[2]").
      appName("file-parse-app").
      config("spark.app.id", "file-parse-app").   // To silence Metrics warning.
      getOrCreate()


    spark.conf.set("spark.executor.cores", "2")
    spark.conf.set("spark.executor.memory", "4g")

    val sc = spark.sparkContext

    //Replace directory for the input file with location of the file on your machine.
    val df = spark.read.format("csv").option("header", false)
      .schema(recordSchema)
      .load(FileUtils.getInputPath(filePath))
    df.show()

    df.createOrReplaceTempView("cancerTable")
    val sqlDF = spark.sql("SELECT sample, bNuc from cancerTable")
    sqlDF.show()
  }

  def loadCsvWithCaseClass(filePath: String) = {
    val spark = SparkSession.builder.
      master("local[2]").
      appName("file-parse-app").
      config("spark.app.id", "file-parse-app").   // To silence Metrics warning.
      getOrCreate()

    spark.conf.set("spark.executor.cores", "2")
    spark.conf.set("spark.executor.memory", "4g")

    val sc = spark.sparkContext

    val cancerRDD = sc.textFile(FileUtils.getInputPath(filePath))
                                    .map(_.split(","))
                                    .map(attributes => CancerClass(attributes(0).trim.toLong, attributes(1).trim.toInt, attributes(2).trim.toInt, attributes(3).trim.toInt, attributes(4).trim.toInt, attributes(5).trim.toInt, attributes(6).trim.toInt, attributes(7).trim.toInt, attributes(8).trim.toInt, attributes(9).trim.toInt, attributes(10).trim.toInt))
    // val cancelDS = cancerRDD.toDS()

    def binarize(s: Int): Int = s match {case 2 => 0 case 4 => 1 }

    spark.udf.register("udfValueToCategory", (arg: Int) => binarize(arg))
    val sqlUDF = spark.sql("SELECT *, udfValueToCategory(clas) from cancerTable")
    sqlUDF.show()

    spark.catalog.currentDatabase
    spark.catalog.isCached("cancerTable")

    spark.catalog.cacheTable("cancerTable")
    spark.catalog.isCached("cancerTable")
    spark.catalog.clearCache
    spark.catalog.listDatabases.show()

    spark.catalog.listDatabases.take(1)
    spark.catalog.listTables.show()
    spark.catalog.dropTempView("cancerTable")
    spark.catalog.listTables.show()
  }

  def loadDataWithRow(filePath: String) = {
    val spark = SparkSession.builder.
      master("local[2]").
      appName("file-parse-app").
      config("spark.app.id", "file-parse-app").   // To silence Metrics warning.
      getOrCreate()

    spark.conf.set("spark.executor.cores", "2")
    spark.conf.set("spark.executor.memory", "4g")

    val sc = spark.sparkContext

    //Code for Understanding Resilient Distributed Datasets (RDDs) section
    //Replace directory for the input file with location of the file on your machine.
    val cancerRDD = sc.textFile(FileUtils.getInputPath(filePath), 4)
    cancerRDD.partitions.size
    import spark.implicits._
    val cancerDF = cancerRDD.toDF()
    cancerDF.show()

    import org.apache.spark.sql.Row
    def row(line: List[String]): Row = { Row(line(0).toLong, line(1).toInt, line(2).toInt, line(3).toInt, line(4).toInt, line(5).toInt, line(6).toInt, line(7).toInt, line(8).toInt, line(9).toInt, line(10).toInt) }
    val data = cancerRDD.map(_.split(",").to[List]).map(row)
    val cancerDF2 = spark.createDataFrame(data, recordSchema)
    cancerDF2.show()
  }

  loadCSVAsDataFrame("cancer-wisconsin.data.txt")
  loadCsvWithCaseClass("cancer-wisconsin.data.txt")
  loadDataWithRow("cancer-wisconsin.data.txt")
}
