package com.wsb.project

import org.apache.spark.sql._
import org.apache.spark.sql.functions.{row_number}
import org.apache.spark.sql.expressions.Window
import spark.implicits._

// Uwaga: dane dot. dróg znajdują się w plikach "mainData<JED_ADM>"
object RoadTransformation {

  val username = "username"

  val spark = SparkSession.builder()
    .appName("RoadTransformation")
    .getOrCreate()

  case class Road(id: BigInt, roadCategory: String, roadType: String)

  def readCsv(path: String) = {
    spark.read.
      format("org.apache.spark.csv").
      option("header", true).
      option("inferSchema", true).
      csv(path).cache()
  }

  def main(args: Array[String]): Unit = {

    val scotlandRoadsPath = s"/user/$username/proj/spark/mainDataScotland.csv"
    val northEnglandRoadsPath = s"/user/$username/proj/spark/mainDataNorthEngland.csv"
    val southEnglandRoadsPath = s"/user/$username/proj/spark/mainDataSouthEngland.csv"

    val scotlandRoads = readCsv(scotlandRoadsPath)
    val northEnglandRoads = readCsv(northEnglandRoadsPath)
    val southEnglandRoads = readCsv(southEnglandRoadsPath)

    val windowSortByRoadCategory = Window.orderBy("road_category")

    scotlandRoads
      .select(
        "road_category",
        "road_type"
      ).union(northEnglandRoads
      .select(
        "road_category",
        "road_type"
      )).union(southEnglandRoads
      .select(
        "road_category",
        "road_type"
      )).dropDuplicates().withColumn("id", row_number().over(windowSortByRoadCategory))
      .select(
        "id",
        "road_category",
        "road_type"
      ).toDF().as[Road].write.insertInto("w_drogi")
  }
}
