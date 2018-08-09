package ai.nodesense.ml


import org.apache.spark.ml.feature.{RegexTokenizer, Tokenizer}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

//https://spark.apache.org/docs/2.2.0/ml-features.html#tokenizer
//Tokenization is the process of taking text (such as a sentence) and
// breaking it into individual terms (usually words).
// A simple Tokenizer class provides this functionality


object  ML03_Tokenizer extends  App {

  val spark = SparkSession.builder()
    .appName("ml-pipe-line")
    .master("local")
    .getOrCreate()

  val sc = spark.sparkContext

  import spark.sqlContext.implicits._

  val sentenceDataFrame = spark.createDataFrame(Seq(
    (0, "Hi I heard about Spark"),
    (1, "I wish Java could use case classes"),
    (2, "Logistic,regression,models,are,neat")
  )).toDF("id", "sentence")

  val tokenizer = new Tokenizer().setInputCol("sentence").setOutputCol("words")
  val regexTokenizer = new RegexTokenizer()
    .setInputCol("sentence")
    .setOutputCol("words")
    .setPattern("\\W") // alternatively .setPattern("\\w+").setGaps(false)

  val countTokens = udf { (words: Seq[String]) => words.length }

  val tokenized = tokenizer.transform(sentenceDataFrame)
  tokenized.select("sentence", "words")
    .withColumn("tokens", countTokens(col("words"))).show(false)

  val regexTokenized = regexTokenizer.transform(sentenceDataFrame)
  regexTokenized.select("sentence", "words")
    .withColumn("tokens", countTokens(col("words"))).show(false)
}
