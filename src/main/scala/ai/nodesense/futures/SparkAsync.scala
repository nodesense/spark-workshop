package ai.nodesense.futures
// Code is based on http://www.russellspitzer.com/2017/02/27/Concurrency-In-Spark/

import org.apache.spark.{SparkConf, SparkContext}

/** A singleton object that controls the parallelism on a Single Executor JVM */
object ThreadedConcurrentContext {
  import scala.util._
  import scala.concurrent._
  import scala.concurrent.duration.Duration
  import scala.concurrent.duration.Duration._
  import scala.concurrent.ExecutionContext.Implicits.global

  /** Wraps a code block in a Future and returns the future */
  def executeAsync[T](f: => T): Future[T] = {
    Future(f)
  }

  /** Awaits only a set of elements at a time. Instead of waiting for the entire batch
    * to finish waits only for the head element before requesting the next future*/
  def awaitSliding[T](it: Iterator[Future[T]], batchSize: Int = 3, timeout: Duration = Inf): Iterator[T] = {
    val slidingIterator = it.sliding(batchSize - 1) //Our look ahead (hasNext) will auto start the nth future in the batch
    val (initIterator, tailIterator) = slidingIterator.span(_ => slidingIterator.hasNext)
    initIterator.map( futureBatch => Await.result(futureBatch.head, timeout)) ++
      tailIterator.flatMap( lastBatch => Await.result(Future.sequence(lastBatch), timeout))
  }
}

/** A singleton object that controls the parallelism on a Single Executor JVM, Using the GlobalContext**/
object ConcurrentContext {
  import java.util.concurrent.Executors
  import scala.util._
  import scala.concurrent._
  import scala.concurrent.ExecutionContext.Implicits.global
  import scala.concurrent.duration.Duration
  import scala.concurrent.duration.Duration._
  /** Wraps a code block in a Future and returns the future */
  def executeAsync[T](f: => T): Future[T] = {
    Future(f)
  }

  /** Awaits an entire sequence of futures and returns an iterator. This will
    wait for all futures to complete before returning**/
  def awaitAll[T](it: Iterator[Future[T]], timeout: Duration = Inf) = {
    Await.result(Future.sequence(it), timeout)
  }

}

object SparkAsync extends App {
  val conf = new SparkConf()
    .setAppName("RddExample")
    .setMaster("local[1]")

  val sc = new SparkContext(conf)

  val prices = List(100, 200, 300, 400, 500, 600, 700, 800, 900);

  def slowFoo[T](x: T):T = {
    println(s"slowFoo start ($x)")
    Thread.sleep(5000)
    println(s"slowFoo end($x)")
    x
  }

  /** Immediately returns the input **/
  def fastFoo[T](x: T): T = {
    println(s"fastFoo($x)")
    x
  }

  // load initial RDD
  val rdd = sc.parallelize(prices)

//  rdd.map(fastFoo)
//    .map(x => ThreadedConcurrentContext.executeAsync(slowFoo(x))) //Async block goes here
//    .mapPartitions( it => ThreadedConcurrentContext.awaitSliding(it)) //
//    .foreach( x => println(s"Finishing with $x"))


  rdd.map(fastFoo)
    .map(x => ConcurrentContext.executeAsync(slowFoo(x)))
    .mapPartitions( it => ConcurrentContext.awaitAll(it))
    .foreach( x => println(s"Finishing with $x"))
}
