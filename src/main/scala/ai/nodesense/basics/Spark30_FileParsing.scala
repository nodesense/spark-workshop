package ai.nodesense.basics
import ai.nodesense.util.FileUtils
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object FileParsing extends  App {
  val spark = SparkSession.builder.
    master("local[2]").
    appName("file-parse-app").
    config("spark.app.id", "file-parse-app").   // To silence Metrics warning.
    getOrCreate()

  val sc = spark.sparkContext

  try {
    // Load one of the religious texts, don't convert each line to lower case
    // this time, then extract the fields in the "book|chapter|verse|text" format
    // used for each line, creating an RDD. However, note that the logic used
    // to split the line will work reliably even if the delimiters aren't present!
    // Note also the output nested tuple. Joins only work for RDDs of
    // (key,value) tuples
    val input = sc.textFile("src/main/resources/data/kjvdat.txt")
      .map { line =>
        val ary = line.split("\\s*\\|\\s*")
        (ary(0), (ary(1), ary(2), ary(3)))
      }

    // The abbreviations file is tab separated, but we only want to split
    // on the first space (in the unlikely case there are embedded tabs
    // in the names!)
    val abbrevs = sc.textFile("src/main/resources/data/abbrevs-to-names.tsv")
      .map { line =>
        val ary = line.split("\\s+", 2)
        (ary(0), ary(1).trim)  // I've noticed trailing whitespace...
      }

    // Cache both RDDs in memory for fast, repeated access.
    input.cache
    abbrevs.cache

    // Join on the key, the first field in the tuples; the book abbreviation.

    val verses = input.join(abbrevs)

    if (input.count != verses.count) {
      println(s"input count, ${input.count}, doesn't match output count, ${verses.count}")
    }

    // Project out the flattened data we want:
    //   fullBookName|chapter|verse|text

    val verses2 = verses.map {
      // Drop the key - the abbreviated book name
      case (_, ((chapter, verse, text), fullBookName)) =>
        (fullBookName, chapter, verse, text)
    }

    val output = "outputs/book-captions"
    println(s"Writing output to: $output")
    FileUtils.rmrf(output)

    verses2.saveAsTextFile("outputs/book-captions")



    val seqOutput = "outputs/sequence-book-captions"

    //TODO: Read/Write as Sequence File


  } finally {
    spark.stop()
  }

}
