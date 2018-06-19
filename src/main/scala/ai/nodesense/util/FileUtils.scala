package ai.nodesense.util

import java.io._
import scala.io.Source

// For local system
object FileUtils {
    case class FileOperationError(msg: String) extends RuntimeException(msg)

    def rmrf(root: String): Unit = rmrf(new File(root))

    def rmrf(root: File): Unit = {
      if (root.isFile) root.delete()
      else if (root.exists) {
        root.listFiles.foreach(rmrf)
        root.delete()
      }
    }

    def rm(file: String): Unit = rm(new File(file))

    def rm(file: File): Unit =
      if (file.delete == false) throw FileOperationError(s"Deleting $file failed!")

    def mkdir(path: String): Unit = (new File(path)).mkdirs

    def getCurrentPath: String = "file://" + System.getProperty("user.dir")

    def getDataDirPath: String = getCurrentPath + "/data/"
    def getOutputDirPath: String = getCurrentPath + "/output/"

    def getInputPath(fileName: String): String =  getDataDirPath + fileName

    def getOutputPath(fileName: String): String = getOutputDirPath + fileName
}
