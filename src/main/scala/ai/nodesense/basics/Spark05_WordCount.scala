package ai.nodesense.basics

import ai.nodesense.util.FileUtils
import org.apache.spark.sql.SparkSession


/*
  Example demonstrate word count,
  using map, filter, flatMap, reduceByKey

  Input: war-and-peace.txt, must be located in data/war-and-peace.txt
  Output: outputs/word-count-results director on project root folder
 */

object WordCount extends App {

  val filepath: String =  FileUtils.getInputPath("war-and-peace.txt")
  val outputFilePath:String = FileUtils.getOutputPath("word-count-results");

    //Create a SparkContext to initialize Spark

    val spark = SparkSession.builder.
      master(s"local").
      appName("word-count-app").
      config("spark.app.id", "word-count-app").   // To silence Metrics warning.
      config("spark.serializer", "org.apache.spark.serializer.KryoSerializer").
      getOrCreate()

    val sc = spark.sparkContext

    println("Loading file", filepath);

    //    // Load the text into a Spark RDD, which is a distributed representation of each line of text
    //val textFile = sc.textFile("src/main/resources/readme.md")
    val textFile = sc.textFile(filepath);

    //word count
    //flapMap transform an array of items
    // into individual item while passing next chain
    // for example, "Hello Spark", split returns ["Hello", "Spark"]
    // but map shall get "Hello" and then "Spark"

    val wordMapRDD = textFile
         .map(line => line.toLowerCase)
        .flatMap(line => line.split("""\s+"""))
        .map( line => line.replaceAll("[-+.“”’‘!?;^:,]",""))
        .filter(!_.isEmpty)
        .map(word => (word, 1)) // converting to tuple, (key, value)


    val wordcountRdd = wordMapRDD
        .reduceByKey( (acc, value) => acc + value)
        // .reduceByKey(_ + _) // reduce by key ie unique word in in RDD, till here we have RDD

    wordcountRdd.foreach(println)
    System.out.println("Total words: " + wordcountRdd.count());

    // Now let us count unique word, ie by value
    val wordCount = wordMapRDD
                    .countByValue() // returns a Map[T, Long] to the driver, no more RDD after this

    //FIXME: Prints ((akka, 1), 1) format, need to transform to word to count
    wordCount.foreach(println)

    val wc2 = wordCount.map(key_value => s"${key_value._1},${key_value._2}").toSeq
    val wc = sc.makeRDD(wc2, 1)

    FileUtils.rmrf(outputFilePath)

    wc.saveAsTextFile(outputFilePath)

}
