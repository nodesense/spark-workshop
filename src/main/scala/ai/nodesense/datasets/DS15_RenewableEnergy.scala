package ai.nodesense.datasets


import ai.nodesense.util.FileUtils
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Encoders

import org.apache.spark.sql.expressions.scalalang.typed.{avg => typedAvg, count => typedCount, sum => typedSum}


case class EnergyPlant(   municipality_code: String,
                          municipality: String,
                          energy_source_level_1: String,
                          energy_source_level_2: String,
                          energy_source_level_3: String,
                          technology: String,
                          electrical_capacity: Double,
                          number_of_installations: Int,
                          lat: Option[Double],
                          lon: Option[Double],
                          data_source: String,
                          comment: String );

object DS15_RenewableEnergy extends App {

  def createSparkSession() = {
    val spark = SparkSession
      .builder
      .master("local[4]")
      .appName("DS15_RenewableEnergy")
      .config("spark.sql.crossJoin.enabled", "true")

      .getOrCreate()

      spark
  }

  def readPowerPlants(spark: SparkSession, filePath:String) = {
    import spark.sqlContext.implicits._

    val powerPlantDS = spark.read
      .option("header", "true") //reading the headers
      .option("mode", "DROPMALFORMED")
      .option("delimiter", ",")
      .option("inferSchema", "true")
      .csv(filePath)
      .as[EnergyPlant]


    powerPlantDS.printSchema()
    println("All PowerPlants")
    powerPlantDS.show(100)
    powerPlantDS
  }


  val spark = createSparkSession()
  import spark.sqlContext.implicits._


  val powerPlantDS = readPowerPlants(spark,   FileUtils.getInputPath("energy/renewable_power_plants_FR.csv"))
  //powerPlantDS.show(100)

  val byStateNameDS = powerPlantDS.groupByKey(body => body.energy_source_level_2)
    .agg(typedCount[EnergyPlant](_.municipality_code).name("count(id)"),
      typedSum[EnergyPlant](_.electrical_capacity).name("sum(electrical_capacity)")
    )
    .withColumnRenamed("value", "group")
    .alias("Summary by energy")

  byStateNameDS.show(100)



}
