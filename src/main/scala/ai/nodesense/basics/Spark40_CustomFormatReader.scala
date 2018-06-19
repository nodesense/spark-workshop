package ai.nodesense.basics
import ai.nodesense.util.{FileUtils, HdfsUtils}

import org.apache.hadoop.mapred.{FileInputFormat, RecordReader}
import org.apache.spark.sql.SparkSession
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.spark.{SparkConf, SparkContext}


case class record(Id: String, Term: String, Rank: String)


object  CustomFormatReader extends  App {
  val spark = SparkSession.builder.
    master("local").
    appName("file-parse-app").
    config("spark.app.id", "file-parse-app").   // To silence Metrics warning.
    getOrCreate()

  val sc = spark.sparkContext
  import spark.sqlContext.implicits._;


  val hconf = new Configuration
  //hconf.set("textinputformat.record.delimiter", "REC")
  hconf.set("textinputformat.record.delimiter", "\n\n")
  val data = sc.newAPIHadoopFile("hdfs://nodesense.local:8020/data/multiline.txt",
    classOf[TextInputFormat], classOf[LongWritable],
    classOf[Text], hconf).map(x => x._2.toString.trim).filter(x => x != "")
    .foreach(record => {
       println(s"***$record*****")
    } )


}

