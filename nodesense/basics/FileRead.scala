package ai.nodesense.basics

import java.io.InputStream

import scala.io.Source

object FileRead extends App {

  def main(): Unit = {
//    val stream : InputStream = getClass.getResourceAsStream("/readme.txt")
//    val lines = scala.io.Source.fromInputStream( stream ).getLines
//
//    println(lines);

    val fileStream = getClass.getResourceAsStream("/readme.txt")
    val lines = Source.fromInputStream(fileStream).getLines
    lines.foreach(line => println(line))
  }

  main();

}
