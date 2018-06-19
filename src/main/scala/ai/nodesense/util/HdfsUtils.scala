package ai.nodesense.util

import java.io.{BufferedReader, InputStreamReader}

import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.spark.SparkContext
import org.apache.hadoop.conf.Configuration
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ListBuffer

//https://www.programcreek.com/scala/org.apache.hadoop.fs.FileSystem

object HdfsUtils {
    val hdfsPath = "hdfs://nodesense.local:8020/"

  private val conf = new Configuration()
  conf.set("fs.defaultFS", hdfsPath)
  //conf.set("mapreduce.jobtracker.address", Constants.Hadoop.JOBTRACKER_ADDRESS)


  def getCurrentPath: String = hdfsPath

  def getDataDirPath: String = getCurrentPath + "/data/"
  def getOutputDirPath: String = getCurrentPath + "/output/"

  def getInputPath(fileName: String): String = getDataDirPath + fileName

  def getOutputPath(fileName: String): String = getOutputDirPath + fileName


  def mkdirs(folderPath: String): Unit = {
    val fs = FileSystem.get(conf)
    val path = new Path(folderPath)
    if (!fs.exists(path)) {
      fs.mkdirs(path)
    }
  }


  def readLinesFromHDFS(fileName: String): Array[String] = {
    val fs = FileSystem.get(conf)
    val path = new Path(fileName)

    val br: BufferedReader = new BufferedReader(new InputStreamReader(fs.open(path)))
    val lines = scala.collection.mutable.ArrayBuffer[String]()
    var line = br.readLine()
    while (line != null) {
      lines += line
      line = br.readLine()
    }
    lines.toArray
  }

  // Note: For production, use SparkContext sc.hadoopConfiguration

  def rmrf(fileName: String, sc: SparkContext) = {
    val fs=FileSystem.get(sc.hadoopConfiguration)
    val outPutPath="/abc"
    if(fs.exists(new Path(outPutPath)))
      fs.delete(new Path(outPutPath),true)
  }

  def pathExists(path: String): Boolean = {
    //val conf = sc.hadoopConfiguration
    val fs = FileSystem.get(conf)
    fs.exists(new Path(path))
  }

  def getFullPath(path:String, sc: SparkContext): String = {
    val conf = sc.hadoopConfiguration
    val fs = FileSystem.get(conf)
    fs.getFileStatus(new Path(path)).getPath().toString
  }

  def getAllFiles(path:String, sc: SparkContext): Seq[String] = {
    val conf = sc.hadoopConfiguration
    val fs = FileSystem.get(conf)
    val files = fs.listStatus(new Path(path))
    files.map(_.getPath().toString)
  }



  def getAllFiles(path:String): Seq[String] = {
    val fs = FileSystem.get(conf)
    val files = fs.listStatus(new Path(path))
    files.map(_.getPath().toString)
  }


  def deleteFilesInHDFS(paths: String*) = {
    paths.foreach { path =>
      val filePath = new Path(path)
      val HDFSFilesSystem = filePath.getFileSystem(conf)
      if (HDFSFilesSystem.exists(filePath)) {
       // logInfo(s"?????$filePath")
        println("File " + filePath + " Exists, Deleting ")
        HDFSFilesSystem.delete(filePath, true)
      } else {
        println("File " + filePath + " not Exists ")
      }
    }
  }


  def saveAsTextFile[T <: Product](rdd: RDD[T], savePath: String) = {
    deleteFilesInHDFS(savePath)
    //logInfo(s"?${savePath}?????")
    rdd.map(_.productIterator.mkString(",")).coalesce(1).saveAsTextFile(savePath)
  }

  def saveAsTextFile(text: String, savePath: String) = {
    deleteFilesInHDFS(savePath)
    //logInfo(s"?${savePath}?????")
    val out = FileSystem.get(conf).create(new Path(savePath))
    out.write(text.getBytes)
    out.flush()
    out.close()
  }


}
