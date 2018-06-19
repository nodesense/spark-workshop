package ai.nodesense.basics

import ai.nodesense.models.Click
import org.joda.time.DateTime
import org.joda.time.format.{DateTimeFormat, DateTimeFormatter};
//import org.joda.DateTimeFormatter;
//import org.joda.DataTimeFormat;

import scala.io.Source
import scala.collection.mutable.ListBuffer;


object ReadFile extends App {

  def readFile(filename: String): ListBuffer[Click] = {

    val clicksList = ListBuffer.empty[Click]

    val formatter: DateTimeFormatter = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");

    for (line <- Source.fromFile(filename).getLines) {
      val points = line.split(",");
      val sessionId = points(0).toInt;
      val timeStamp :DateTime = DateTime.parse(points(1), formatter);
      val itemId = points(2).toInt;
      val category = points(3).toInt;

      val click = Click(sessionId, timeStamp, itemId, category);
      clicksList += click;

      //println(sessionId, timeStamp, itemId, category)
      //println(click);
    }

    return clicksList;
  }



  val filename = "/Users/krish/dataset/yoochoose-data/clicks/test.csv"

  readFile(filename);
  //val filename = "/Users/krish/dataset/yoochoose-data/clicks/yoochoose-clicks.csv.aa";

}
