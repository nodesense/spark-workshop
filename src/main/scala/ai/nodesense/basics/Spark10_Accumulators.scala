package ai.nodesense.basics
import ai.nodesense.util.FileUtils
import org.apache.spark._

// Accumulators are useful to collect the information from
// clusters/nodes running across all networks

// Use cases: to get over all completion percentages of tasks
// to know errors/warinings/information related to data processing

// Spark 2.x onwards, we should use longAccumulator/doubleAccumulator
// or collectionaccumulators, rdd.accumulator is deprecated.

object RddAccumulators extends App {
  val sc = new SparkContext("local",
                            "BasicLoadNums",
                              System.getenv("SPARK_HOME"))


  val inputFilePath = FileUtils.getInputPath("student-grades.txt")
   
  val file = sc.textFile(inputFilePath)


  //file.foreach(println(_));
  val counts2 = file.flatMap(line => line.split(" "));


  println("counts 2", counts2);

  val errorsAcc = sc.longAccumulator("errors");
  val dataAcc = sc.longAccumulator("data");


  // Very bad idea to use local variables
  //var k : Int = 0;

   val counts = file.flatMap(line => {
    try {
      val input = line.split(" ")
      // k += 1 // bad, since K is local variable, not suitable for distributed
      val data = Some((input(0), input(1).toInt))
       dataAcc.add(1)
      data
    } catch {
      case e: java.lang.NumberFormatException => {
        errorsAcc.add(1)
         None
      }
      case e: java.lang.ArrayIndexOutOfBoundsException => {
        errorsAcc.add(1)
         None
      }
    }
  }).reduceByKey(_ + _)

  counts.take(10).foreach(println)

  println("Error Lines Accumulated ", errorsAcc.value)

  println("Data Lines Accumulated ", dataAcc.value)

  if (errorsAcc.value < 0.1 * dataAcc.value) {
    println(s"Not many errors ${errorsAcc.value} for ${dataAcc.value} records")

    counts.saveAsTextFile("output.txt")
  } else {
    println(s"Too many errors ${errorsAcc.value} for ${dataAcc.value} records")
  }
}
