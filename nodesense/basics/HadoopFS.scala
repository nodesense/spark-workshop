package ai.nodesense.basics

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path, FSDataOutputStream, FSDataInputStream}
import org.apache.commons.io.{IOUtils};

import java.io.FileInputStream
import java.net.URI


object HadoopFS extends App {

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

    val conf = new Configuration()
     conf.set("fs.defaultFS", url)
     System.setProperty("HADOOP_USER_NAME", "root")
    System.setProperty("hadoop.home.dir", "/");
    val fs: FileSystem = FileSystem.get(URI.create(url), conf);

    val workingDir: Path=fs.getWorkingDirectory();

     val newFolderPath:Path = new Path("/user/root/myapps");
      if(!fs.exists(newFolderPath)) {
         println("Folder does not exist ")
         // Create new Directory
         fs.mkdirs(newFolderPath);
         println("created dir", newFolderPath)
      } else {
        println("Folder exists ")
      }



       println("Begin Write file into hdfs");
      //Create a path
      val hdfswritepath: Path = new Path(newFolderPath + "/" + "names.txt");
      //Init output stream
      val outputStream: FSDataOutputStream=fs.create(hdfswritepath);
      //Cassical output stream usage
      val fileContent = "hello;world";
      outputStream.writeBytes(fileContent);
      outputStream.close();
      println("End Write file into hdfs");

      println("Read file into hdfs");
      //Create a path
      val hdfsreadpath: Path = new Path(newFolderPath + "/" + "names.txt");
      //Init input stream
      val inputStream: FSDataInputStream = fs.open(hdfsreadpath);
      //Classical input stream usage
      val out: String= IOUtils.toString(inputStream, "UTF-8");
      println("File content", out);
      inputStream.close();
      fs.close();

      //------------------

    val hdfs = FileSystem.get(new URI(url), conf)
   
    val path = new Path(filePath)
    
   
    val stream = hdfs.open(path)
    def readLines = Stream.cons(stream.readLine, Stream.continually( stream.readLine))

     
    val snapshot_id : String = readLines.takeWhile(_ != null).mkString("\n")
    println("read ", snapshot_id)
    //readLines.takeWhile(_ != null).foreach(line => println(line))

    println("reading done");

  }

  write("hdfs://sandbox-hdp.hortonworks.com:8020", "/user/root/test.txt", "Hello World".getBytes)

  read("hdfs://sandbox-hdp.hortonworks.com:8020", "/user/root/test.txt")

}