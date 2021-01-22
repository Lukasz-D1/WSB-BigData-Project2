package com.wsb.project

import org.apache.spark.sql._
import org.apache.spark.sql.functions.{col, monotonically_increasing_id}
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

  case class roadType (id: BigInt,
                       road_category: String,
                       road_type: String)

  import spark.implicits._

  def main(args: Array[String]): Unit = {

    val scotlandRoadsPath = s"/user/$username/proj/spark/mainDataScotland.csv"
    val northEnglandRoadsPath = s"/user/$username/proj/spark/mainDataNorthEngland.csv"
    val southEnglandRoadsPath = s"/user/$username/proj/spark/mainDataSouthEngland.csv"

    val scotlandRoads_ds = readCsv(scotlandRoadsPath)
    val northEnglandRoads_ds = readCsv(northEnglandRoadsPath)
    val southEnglandRoads_ds = readCsv(southEnglandRoadsPath)

    val dataUnion = scotlandRoads_ds.select(scotlandRoads_ds("road_category"), scotlandRoads_ds("road_type")).
      union(northEnglandRoads_ds.select(northEnglandRoads_ds("road_category"), northEnglandRoads_ds("road_type")).
        union(southEnglandRoads_ds.select(southEnglandRoads_ds("road_category"), southEnglandRoads_ds("road_type")))).
      distinct()


    val roadsToWrite =
      dataUnion.withColumn("id", monotonically_increasing_id())
        .select(col("id").alias("id"), col("road_category"), col("road_type")
        ).as[roadType]

    roadsToWrite.write.insertInto("d_roads")
  }
}
