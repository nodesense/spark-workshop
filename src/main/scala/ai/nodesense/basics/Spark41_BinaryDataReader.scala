package ai.nodesense.basics

import java.io.{File, FileFilter}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{BytesWritable, LongWritable}
import org.apache.hadoop.io.{LongWritable, Text}

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.{InputSplit, JobContext, RecordReader, TaskAttemptContext}
import org.apache.spark.sql.SparkSession

/**
  * Custom Input Format for reading and splitting flat binary files that contain records, each of which
  * are a fixed size in bytes. The fixed record size is specified through a parameter recordLength
  * in the Hadoop configuration.
  */

object FixedLengthBinaryInputFormat {

  /**
    * This function retrieves the recordLength by checking the configuration parameter
    *
    */
  def getRecordLength(context: JobContext): Int = {

    // retrieve record length from configuration
    context.getConfiguration.get("recordLength").toInt
  }

}

class FixedLengthBinaryInputFormat extends FileInputFormat[LongWritable, BytesWritable] {


  /**
    * Override of isSplitable to ensure initial computation of the record length
    */
  override def isSplitable(context: JobContext, filename: Path): Boolean = {

    if (recordLength == -1) {
      recordLength = FixedLengthBinaryInputFormat.getRecordLength(context)
    }
    if (recordLength <= 0) {
      println("record length is less than 0, file cannot be split")
      false
    } else {
      true
    }

  }

  /**
    * This input format overrides computeSplitSize() to make sure that each split
    * only contains full records. Each InputSplit passed to FixedLengthBinaryRecordReader
    * will start at the first byte of a record, and the last byte will the last byte of a record.
    */
  override def computeSplitSize(blockSize: Long, minSize: Long, maxSize: Long): Long = {

    val defaultSize = super.computeSplitSize(blockSize, minSize, maxSize)

    // If the default size is less than the length of a record, make it equal to it
    // Otherwise, make sure the split size is as close to possible as the default size,
    // but still contains a complete set of records, with the first record
    // starting at the first byte in the split and the last record ending with the last byte

    defaultSize match {
      case x if x < recordLength => recordLength.toLong
      case _ => (Math.floor(defaultSize / recordLength) * recordLength).toLong
    }
  }

  /**
    * Create a FixedLengthBinaryRecordReader
    */
  override def createRecordReader(split: InputSplit, context: TaskAttemptContext):
  RecordReader[LongWritable, BytesWritable] = {
    new FixedLengthBinaryRecordReader
  }

  var recordLength = -1

}

import java.io.IOException
import org.apache.hadoop.fs.FSDataInputStream
import org.apache.hadoop.io.{BytesWritable, LongWritable}
import org.apache.hadoop.io.compress.CompressionCodecFactory
import org.apache.hadoop.mapreduce.InputSplit
import org.apache.hadoop.mapreduce.lib.input.FileSplit
import org.apache.hadoop.mapreduce.RecordReader
import org.apache.hadoop.mapreduce.TaskAttemptContext

/**
  *
  * FixedLengthBinaryRecordReader is returned by FixedLengthBinaryInputFormat.
  * It uses the record length set in FixedLengthBinaryInputFormat to
  * read one record at a time from the given InputSplit.
  *
  * Each call to nextKeyValue() updates the LongWritable KEY and BytesWritable VALUE.
  *
  * KEY = record index (Long)
  * VALUE = the record itself (BytesWritable)
  *
  */
class FixedLengthBinaryRecordReader extends RecordReader[LongWritable, BytesWritable] {

  override def close() {
    if (fileInputStream != null) {
      fileInputStream.close()
    }
  }

  override def getCurrentKey: LongWritable = {
    recordKey
  }

  override def getCurrentValue: BytesWritable = {
    recordValue
  }

  override def getProgress: Float = {
    splitStart match {
      case x if x == splitEnd => 0.0.toFloat
      case _ => Math.min(((currentPosition - splitStart) / (splitEnd - splitStart)).toFloat, 1.0).toFloat
    }
  }

  override def initialize(inputSplit: InputSplit, context: TaskAttemptContext) {

    // the file input
    val fileSplit = inputSplit.asInstanceOf[FileSplit]

    // the byte position this fileSplit starts at
    splitStart = fileSplit.getStart

    // splitEnd byte marker that the fileSplit ends at
    splitEnd = splitStart + fileSplit.getLength

    // the actual file we will be reading from
    val file = fileSplit.getPath

    // job configuration
    val job = context.getConfiguration

    // check compression
    val codec = new CompressionCodecFactory(job).getCodec(file)
    if (codec != null) {
      throw new IOException("FixedLengthRecordReader does not support reading compressed files")
    }

    // get the record length
    recordLength = FixedLengthBinaryInputFormat.getRecordLength(context)

    // get the filesystem
    val fs = file.getFileSystem(job)

    // open the File
    fileInputStream = fs.open(file)

    // seek to the splitStart position
    fileInputStream.seek(splitStart)

    // set our current position
    currentPosition = splitStart

  }

  // .mep (record)

  override def nextKeyValue(): Boolean = {

    if (recordKey == null) {
      recordKey = new LongWritable()
    }

    // the key is a linear index of the record, given by the
    // position the record starts divided by the record length
    recordKey.set(currentPosition / recordLength)

    // the recordValue to place the bytes into
    if (recordValue == null) {
      recordValue = new BytesWritable(new Array[Byte](recordLength))
    }

    // read a record if the currentPosition is less than the split end
    if (currentPosition < splitEnd) {

      // setup a buffer to store the record
      val buffer = recordValue.getBytes

      fileInputStream.read(buffer, 0, recordLength)

      // update our current position
      currentPosition = currentPosition + recordLength

      // return true
      return true
    }

    false
  }

  var splitStart: Long = 0L
  var splitEnd: Long = 0L
  var currentPosition: Long = 0L
  var recordLength: Int = 0
  var fileInputStream: FSDataInputStream = null
  var recordKey: LongWritable = null
  var recordValue: BytesWritable = null

}


object  BinaryDataReader extends  App {
  val spark = SparkSession.builder.
    master("local[2]").
    appName("file-parse-app").
    config("spark.app.id", "file-parse-app"). // To silence Metrics warning.
    getOrCreate()

  val sc = spark.sparkContext

  import spark.sqlContext.implicits._;

  val hconf = new Configuration
  hconf.set("textinputformat.record.delimiter", "REC")
  hconf.setInt("recordLength", 4)

  val data = sc.newAPIHadoopFile("hdfs://nodesense.local:8020/data/orders.csv",
    classOf[FixedLengthBinaryInputFormat],
    classOf[LongWritable],
    classOf[BytesWritable],
    hconf)
    .map (x => x._2)
    .foreach(println(_))


}