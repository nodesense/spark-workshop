package ai.nodesense.basics

import ai.nodesense.util.FileUtils
import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.feature.StopWordsRemover

//FIXME: Do not run this file now, some issue with
// col type mismatch, shall be fixed in github later

/* This file
object Spark05_WordCountStopWords extends App {

  val filepath: String =  FileUtils.getInputPath("war-and-peace.txt")
  val outputFilePath:String = FileUtils.getOutputPath("word-count-results");

    //Create a SparkContext to initialize Spark

    val spark = SparkSession.builder.
      master(s"local").
      appName("word-count-app").
      config("spark.app.id", "word-count-app").   // To silence Metrics warning.
      config("spark.serializer", "org.apache.spark.serializer.KryoSerializer").
      getOrCreate()

   import spark.sqlContext.implicits._

    val sc = spark.sparkContext

    println("Loading file", filepath);

    //    // Load the text into a Spark RDD, which is a distributed representation of each line of text
    //val textFile = sc.textFile("src/main/resources/readme.md")
    val textFile = sc.textFile(filepath);

    //word count
    val wordMapDF = textFile
         .map(line => line.toLowerCase)
        .flatMap(line => line.split(" "))
       // .map( line => line.replaceAll("[-+.“”’‘!?;^:,]",""))
       // .filter(!_.isEmpty)
        //.map(word => (word, 1)) // converting to tuple, (key, value)
      .toDF("words" )

  val remover = new StopWordsRemover()
    .setInputCol("words")
    .setOutputCol("removed")
    .setStopWords(Array("the","a","http","i","me","to","what","in","rt"))

  val newDataSet = remover.transform(wordMapDF)

  newDataSet
    .select("removed") // if you're interested only in "clean" text
    .map(row => row.getSeq[String](0).mkString(" ")) // make Array[String] into String
    .write.text(outputFilePath)

    newDataSet.foreach(println(_))

    FileUtils.rmrf(outputFilePath)

    //wc.saveAsTextFile(outputFilePath)




}
*/