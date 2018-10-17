package ai.nodesense.basics

import org.apache.spark.{SparkConf, SparkContext}

/* the code demonstrate simple RDD fold aggregation
   Like finding total, but start with seed value
* Input: Inline Code
* Output: Console output
* */


object Spark03_Fold extends App {
  val conf = new SparkConf()
    .setAppName("Spark Hello World")
    .setMaster("local") // single thread

  val sc = new SparkContext(conf)

  val data = Array(0, 1, 2, 3, 4, 5, 6, 7, 8, 9)
  val distData = sc.parallelize(data)

  // fold(0) starts with 0 count
  // acc is an accumulator, collect the results on every step
  // n is the value from collection 0, 1, 2, ...9
  // (acc, n) => acc + n method is called with
  // (0, 1), (1, 2), (3, 3), (6, 4) (10, 5).....
  val foldedRdd = distData.fold(0) ( (acc, n) => acc + n)

  println("In Int", foldedRdd.intValue())
  println("In Double", foldedRdd.doubleValue())

  // but how different fold from sum()?
  // It is not that fold to be used only with numbers/sum

  //Fold with factorial

  val numbers = sc.parallelize((1 to 6).toList)

  val results = numbers.fold(1) ((acc, i) => {
    println(s"Acc $acc, I $i")
    acc * i
  })

  println("Factorial ", results.intValue())

  // Fold with Option
  // Say if you have clean data and bad data, want to decide
  // custom average leaving bad data

  case class Meeting(attendance: Int)
  // Let us find total number of people attented meetings
  // where as some meeting had participants
  // some meetings with 0
  // some meeting data are null, no object found
  // some meetings had wrong data -1
  val allMeetings = List( Meeting (10),
    Meeting(5),
    Meeting(-3), // should not counted, wrong data
    Meeting(0), // should not be counted
    Meeting(20)
  )

  // Fold expects same data type used in fold as return as well
  val results2 = allMeetings.fold(Meeting(0)) ((acc, meeting) => {
    println(s"Meeting $acc, Meeting $meeting")
    // don't count if attendance is 0 or less
    if (meeting.attendance <= 0)  acc
    else Meeting(acc.attendance + meeting.attendance)
  })


  val meetings = sc.parallelize(allMeetings);

  val results3 = meetings.fold(Meeting(0)) ((acc, meeting) => {
    println(s"Meeting $acc, Meeting $meeting")
    // don't count if attendance is 0 or less
    if (meeting.attendance <= 0)  acc
    else Meeting(acc.attendance + meeting.attendance)
  })

  println("Meeting results ", results2)

  println("Meeting Spark results ", results3)

  println("Stopping")
  sc.stop()
}



