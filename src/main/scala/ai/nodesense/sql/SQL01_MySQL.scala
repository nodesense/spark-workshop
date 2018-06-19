package ai.nodesense.sql

import java.util.Properties

import org.apache.spark.sql.{Dataset,Row}
import org.apache.spark.sql.SparkSession


object SQL01_MySQL extends  App {


  val spark: SparkSession = SparkSession.builder.appName ("Spark SQL Test").master ("local[*]").getOrCreate

  val connectionProperties: Properties = new Properties()
  connectionProperties.put ("driver", "com.mysql.jdbc.Driver")
  connectionProperties.put ("url", "jdbc:mysql://localhost:3306/productsdb")
  connectionProperties.put ("user", "root")
  connectionProperties.put ("password", "")

  val jdbcDF: Dataset[Row] = spark
                            .read
                            .jdbc (connectionProperties.getProperty ("url"),
                              "products",
                                    connectionProperties)
  jdbcDF.show ()

  jdbcDF.printSchema();

  val appendSql:Dataset[Row] = spark.sql("INSERT INTO products VALUES('Spark 1' , 10000, 15)");


}
