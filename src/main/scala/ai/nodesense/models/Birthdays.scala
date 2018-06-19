package ai.nodesense.models

import java.sql.Timestamp


case class Birthdays(id: Int,
                     state:String,
                     year: Int,
                     month: Int,
                     day :Int,
                     date: Timestamp,
                     wday: String,
                     births: Int)
