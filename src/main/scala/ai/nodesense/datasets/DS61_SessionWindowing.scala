package ai.nodesense.datasets

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

import scala.collection.mutable

object SessionWindowing {
  case class SessionDefinition(group:String, sessionStart:Long, sessionEnd:Long, sessionLength:Long, sessionEvents:Int)
  case class JsonLeadLag(group:String, ts:Long, value:Long)


  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  def main(args:Array[String]): Unit = {
    val sessionJson = "src/main/resources/data/session.json"
    val timeGap = 5

    val isLocal = true

    val sparkSession = if (isLocal) {
      SparkSession.builder
        .master("local")
        .appName("my-spark-app")
        .config("spark.some.config.option", "config-value")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.sql.parquet.compression.codec", "gzip")
       // .enableHiveSupport()
        .getOrCreate()
    } else {
      SparkSession.builder
        .appName("my-spark-app")
        .config("spark.some.config.option", "config-value")
    ///    .enableHiveSupport()
        .getOrCreate()
    }
    println("---")

    import sparkSession.implicits._

    val sessionDs = sparkSession.read.json(sessionJson).as[JsonLeadLag]

    sessionDs.createOrReplaceTempView("session_table")

    sparkSession.sql("select * from session_table").collect().foreach(println)

    val sessionDefinitinonDf = sessionDs.rdd.map(r => {
      (r.group, r)
    }).groupByKey().flatMap{ case (group, jsonObjIt) =>

      var lastStart:Long = -1
      var lastEnd:Long = -1
      var sessionCount = 1
      var eventsInASession = 0

      val sessionList = new mutable.MutableList[SessionDefinition]

      jsonObjIt.toSeq.sortBy(r => r.ts).foreach(record => {
        val ts = record.ts
        eventsInASession += 1

        if (lastStart == -1) {
          lastStart = ts
        } else if (ts > lastEnd + timeGap) {
          sessionList += SessionDefinition(group, lastStart, lastEnd, lastEnd - lastStart, eventsInASession)
          lastStart = ts
          eventsInASession = 0
        }
        lastEnd = ts
      })
      sessionList
    }

    sessionDefinitinonDf.collect().foreach(println)

  }
}

