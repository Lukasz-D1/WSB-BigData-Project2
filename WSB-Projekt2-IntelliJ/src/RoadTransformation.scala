package com.wsb.project

import org.apache.spark.sql._
import org.apache.spark.sql.expressions.Window

// Uwaga: dane dot. dróg znajdują się w plikach "mainData<JED_ADM>"
object RoadTransformation {

  val spark = SparkSession.builder()
    .appName("RoadTransformation")
    .getOrCreate()

  import spark.implicits._

  case class Road(id: Integer, roadCategory: String, roadType: String)

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

    val scotlandRoads = readCsv(scotlandRoadsPath).cache()
    val northEnglandRoads = readCsv(northEnglandRoadsPath).cache()
    val southEnglandRoads = readCsv(southEnglandRoadsPath).cache()

    val windowSortByRoadCategory = Window.orderBy("road_category")

    val scotlandRoadsWithNoIds = scotlandRoads
      .select(
        "road_category",
        "road_type"
      ).dropDuplicates()

    val northEnglandRoadsWithNoIds = northEnglandRoads
      .select(
        "road_category",
        "road_type"
      ).dropDuplicates()

    val southEnglandRoadsWithNoIds = southEnglandRoads
      .select(
        "road_category",
        "road_type"
      ).dropDuplicates()

    val collectedRoadsWithNoIds = scotlandRoadsWithNoIds
      .union(northEnglandRoadsWithNoIds).union(southEnglandRoadsWithNoIds).dropDuplicates()

    val collectedRoadsWithIds = collectedRoadsWithNoIds.withColumn("id", row_number().over(windowSortByRoadCategory))
      .select(
        "id",
        "road_category",
        "road_type"
      ).toDF().as[Road].write.insertInto("w_drogi")
  }
}
