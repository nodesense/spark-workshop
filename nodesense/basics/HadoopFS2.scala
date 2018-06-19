package ai.nodesense.basics

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import java.io.FileInputStream
import java.net.URI


object HadoopFS2 extends App {

  def write(uri: String, filePath: String, data: Array[Byte]) = {
    System.setProperty("HADOOP_USER_NAME", "root")
    println("Writing file")
    val path = new Path(filePath)
    val conf = new Configuration()
    conf.set("fs.defaultFS", uri)

    val fs = FileSystem.get(conf)
    val os = fs.create(path)
    os.write(data)
    fs.close()
    println("Wrote file to hdfs")


  }

  def read(url:String, filePath: String) = {

    println("Reading file")
    //This example checks line for null and prints every existing line consequentally

    val hdfs = FileSystem.get(new URI(url), new Configuration())
    val path = new Path(filePath)
    val stream = hdfs.open(path)
    def readLines = Stream.cons(stream.readLine, Stream.continually( stream.readLine))

    readLines.takeWhile(_ != null).foreach(line => println(line))


  }

  //write("hdfs://sandbox-hdp.hortonworks.com:8020", "/user/root/test2.txt", "Hello World".getBytes)

  read("hdfs://sandbox-hdp.hortonworks.com:8020", "/user/root/test.txt")

}