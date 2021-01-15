package com.wsb.project

// Uwaga: dane dot. dróg znajdują się w plikach "mainData<JED_ADM>"
object RoadTransformation {

  val spark = SparkSession.builder()
    .appName("RoadTransformation")
    .getOrCreate()


  def readCsv(path: String) = {
    spark.read.
      format("org.apache.spark.csv").
      option("header", true).
      option("inferSchema", true).
      csv(path).cache()
  }

  def main(args: Array[String]): Unit = {

    val username = "username"

    val scotlandRoadsPath = s"/user/$username/proj/spark/mainDataScotland.csv"
    val northEnglandRoadsPath = s"/user/$username/proj/spark/mainDataNorthEngland.csv"
    val southEnglandRoadsPath = s"/user/$username/proj/spark/mainDataSouthEngland.csv"

    val scotlandRoads = readCsv(scotlandRoadsPath)
    val northEnglandRoads = readCsv(northEnglandRoadsPath)
    val southEnglandRoads = readCsv(southEnglandRoadsPath)

    scotlandRoads.withColumn("id", monotonically_increasing_id())
      .select(
        "id",
        "road_category",
        "road_type"
      ).write.insertInto("w_drogi")

    northEnglandRoads.withColumn("id", monotonically_increasing_id())
      .select(
        "id",
        "road_category",
        "road_type"
      ).write.insertInto("w_drogi")

    southEnglandRoads.withColumn("id", monotonically_increasing_id())
      .select(
        "id",
        "road_category",
        "road_type"
      ).write.insertInto("w_drogi")
  }
}
