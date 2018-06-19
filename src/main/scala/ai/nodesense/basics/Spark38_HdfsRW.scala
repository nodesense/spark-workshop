package ai.nodesense.basics

import org.apache.log4j.LogManager
import org.apache.spark.sql.{SaveMode, SparkSession}

object HdfsReadWrite {

  case class HelloWorld(message: String)

  def main(args: Array[String]): Unit = {

    val log = LogManager.getRootLogger
    // Creation of Spark Session

    val sparkSession = SparkSession.builder()
                                    .appName("spark-hdfs")
                                      .master("local")
                                    .getOrCreate()

    import sparkSession.implicits._

    //val hdfs_master = args(0)
    val hdfs_master = "hdfs://nodesense.local:8020/";
    // ====== Creating a dataframe with 1 partition
    val df = Seq(HelloWorld("hello world")).toDF().coalesce(1)

    // ======= Writing files
    // Writing file as parquet
    df.write.mode(SaveMode.Overwrite).parquet(hdfs_master + "data/testwiki.par")
    //df.write.parquet(hdfs_master + "user/hdfs/wiki/testwiki")
    //  Writing file as csv
    df.write.mode(SaveMode.Overwrite).csv(hdfs_master + "data/testwiki.csv")
    //df.write.csv(hdfs_master + "user/hdfs/wiki/testwiki.csv")

    // ======= Reading files
    // Reading parquet files
    val df_parquet = sparkSession.read.parquet(hdfs_master + "data/testwiki.par")
    log.info(df_parquet.show())
    //  Reading csv files
    val df_csv = sparkSession.read.option("inferSchema", "true").csv(hdfs_master + "data/testwiki.csv")
    log.info(df_csv.show())
  }
}