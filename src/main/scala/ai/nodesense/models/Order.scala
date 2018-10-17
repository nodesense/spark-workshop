package ai.nodesense.models

import java.io.StringReader
import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.Date

import au.com.bytecode.opencsv.CSVReader
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat

case class Order(invoiceNo : String,
                 stockCode : String,
                 description: String,
                 quantity: Int,
                 invoiceDate: Timestamp,
                 unitPrice: Double,
                 customerId: Option[Int],
                 country: String) {
}

object Order {

  val dateFormat:SimpleDateFormat = new SimpleDateFormat(
    "dd/MM/yy hh:mm");

  //Fixme: Too many error with original data, missing a lot of elements
  // Fixme : Option[Order] has some issue with dataset type system
  def buildFromLine(line: String): Order = {

    //val words = line.split(",")


    val reader = new CSVReader(new StringReader(line))
    val words = reader.readNext()

    val parsedTimeStamp: Date = dateFormat.parse(words(4));

    val timestamp: Timestamp = new Timestamp(parsedTimeStamp.getTime());


    // Found, invoice number has got wrong data
    try {
      Order(
        words(0).trim,
        //words(0).trim.replaceAll("[^0-9]", "").toInt,
        words(1).trim,
        words(2).trim,
        words(3).trim.toInt,
        timestamp,
        words(5).trim.toDouble,
        Some(words(6).trim.toInt),
        words(7).trim
      )
    }catch {
      case _ => {
         // println("Found Error while Processing ", line)
          null
      }
    }

  }
}