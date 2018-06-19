package ai.nodesense.basics

import java.sql.{Connection, DriverManager}


object MySql extends  App {

  println("mysql")

  val url = "jdbc:mysql://localhost:3306/solardb"
  //val driver = "com.mysql.jdbc.Driver"
  val driver = "com.mysql.cj.jdbc.Driver"
  val username = "root"
  val password = ""
  var connection:Connection = _
  try {
    Class.forName(driver)
    connection = DriverManager.getConnection(url, username, password)
    val statement = connection.createStatement
    val rs = statement.executeQuery("SELECT name, description FROM devices")
    while (rs.next) {
      val name = rs.getString("name")
      val description = rs.getString("description")
      println("name = %s, description = %s".format(name,description))
    }
  } catch {
    case e: Exception => e.printStackTrace
  }
  connection.close
}