package ai.nodesense.basics

import java.util.Date

import play.api.libs.json._;

abstract class Measurement ()

case class Voltage(sensor: String,
                   volt: Int,
                   time: Long)
  extends Measurement

case class Watt (sensor: String,
                 watt: Int,
                 time: Long)
  extends Measurement


object MeasurementTest extends App{

  implicit val VoltageFormater = Json.format[Voltage]
  implicit val WattFormatter = Json.format[Watt]

  def test() {
    var watt = Watt("Sensor 1", 100, new Date().getTime());
    println(processMessage(watt));

    var volt = Voltage("Sensor 2", 1000, new Date().getTime());
    println(processMessage(volt));

    import scala.collection.mutable.ListBuffer
    import scala.util.Random

    var list: ListBuffer[Measurement] = new ListBuffer();
    var rnd = new Random;
    //
    //        for (i: Int <- 1 to 20) {
    //          var w = Watt("Sensor 1", rnd.nextInt(100), new Date().getTime());
    //          list.append(w);
    //        }
    //
    //
    //
    //        for (i: Int <- 1 to 20) {
    //          var v = Voltage("Sensor 1", rnd.nextInt(50), new Date().getTime());
    //          list.append(v);
    //        }

    def filterVoltage(measurement: Measurement) : Boolean = {
      println("Called ");

      var result = measurement match {
        case Voltage(sensor: String,volt: Int,time: Long) =>
          true

        case _ => false
      }

      println(result);
      result
    }

    println("Total ", list.length);
    val filteredResult = list.filter(filterVoltage)
    println("Total vaoltage", filteredResult.length);

    var totalVoltage = (acc: Int, measurement: Measurement )  => {
      measurement.asInstanceOf[Voltage].volt + acc
    }



    filteredResult.foldLeft(0)(totalVoltage);

    println("sum ", filteredResult.foldLeft(0)(totalVoltage));

  }

  def processMessage(measurement: Measurement) : String = {
    measurement match {
      case Voltage(sensor: String,volt: Int,time: Long) =>
        s"($sensor $volt $time)"

      case Watt(sensor: String,watt: Int,time: Long) =>
        s"($sensor $watt)"
    }
  }

  test();

}







