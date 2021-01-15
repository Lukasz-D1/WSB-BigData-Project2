package com.wsb.project

import org.apache.spark.sql._
import org.apache.spark.sql.expressions.Window

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

    val windowSortByRoadCategory = Window.orderBy("road_category")

    val scotlandRoadsWithNoIds = scotlandRoads
      .select(
        "road_category",
        "road_type"
      ).dropDuplicates()

    // TODO: sprawdzić jak zadziała select ("*")/select (*) - czy można pobrać wszystkie kolumny z DF (włącznie z dodaną)
    scotlandRoadsWithNoIds.withColumn("id", row_number().over(windowSortByRoadCategory))
      .select(
        "id",
        "road_category",
        "road_type"
      ).write.insertInto("w_drogi")

    val northEnglandRoadsWithNoIds = northEnglandRoads
      .select(
        "road_category",
        "road_type"
      ).dropDuplicates()

    northEnglandRoadsWithNoIds.withColumn("id", row_number().over(windowSortByRoadCategory))
      .select(
        "id",
        "road_category",
        "road_type"
      ).write.insertInto("w_drogi")

    val southEnglandRoadsWithNoIds = southEnglandRoads
      .select(
        "road_category",
        "road_type"
      ).dropDuplicates()

    southEnglandRoadsWithNoIds.withColumn("id", row_number().over(windowSortByRoadCategory))
      .select(
        "id",
        "road_category",
        "road_type"
      ).write.insertInto("w_drogi")
  }
}
