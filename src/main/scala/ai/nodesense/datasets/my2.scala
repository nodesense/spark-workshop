
package org.apache.spark.sql


object my2 {
  import org.apache.spark.rdd.RDD
  import org.apache.spark.sql.{DataFrame, SparkSession}
  import org.apache.spark.sql.catalyst.InternalRow
  import org.apache.spark.sql.types.StructType
  def createDataFrame(spark: SparkSession, catalystRows: RDD[InternalRow], schema: StructType): DataFrame = {
    spark.internalCreateDataFrame(catalystRows, schema)
  }
}
