package com.wsb.project

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession

object GeographyTransformation {

  val spark = SparkSession.builder()
    .appName("GeographyTransformation")
    .getOrCreate()


  def readCsv(path: String): DataFrame = {
    spark.read.
      format("org.apache.spark.csv").
      option("header", true).
      option("inferSchema", true).
      csv(path)
  }

  case class Region(region_id: Int, region_name: String, region_ons_code: String)
  case class Authority(local_authority_ons_code: String,
                       local_authority_id: Int,
                       local_authority_name: String,
                       region_ons_code: String)

  import spark.implicits._

  def main(args: Array[String]): Unit = {

    val username = "username"

    val scotlandAuthoritiesPath = s"/user/$username/proj/spark/authoritiesScotland.csv"
    val scotlandRegionsPath = s"/user/$username/proj/spark/regionsScotland.csv"

    val northEnglandAuthoritiesPath = s"/user/$username/proj/spark/authoritiesNorthEngland.csv"
    val northEnglandRegionsPath = s"/user/$username/proj/spark/regionsNorthEngland.csv"

    val southEnglandAuthoritiesPath = s"/user/$username/proj/spark/authoritiesSouthEngland.csv"
    val southEnglandRegionsPath = s"/user/$username/proj/spark/regionsSouthEngland.csv"

    val scotlandAuthorities = readCsv(scotlandAuthoritiesPath).as[Authority].cache()

    val scotlandRegions = readCsv(scotlandRegionsPath).as[Region].cache()

    val northEnglandAuthorities = readCsv(northEnglandAuthoritiesPath).as[Authority].cache()

    val northEnglandRegions = readCsv(northEnglandRegionsPath).as[Region].cache()

    val southEnglandAuthorities = readCsv(southEnglandAuthoritiesPath).as[Authority].cache()

    val southEnglandRegions = readCsv(southEnglandRegionsPath).as[Region].cache()

    scotlandAuthorities.join(scotlandRegions,
      scotlandAuthorities("region_ons_code") === scotlandRegions("region_ons_code")).
      select(
        scotlandAuthorities("local_authority_ons_code"),
        scotlandAuthorities("local_authority_name"),
        scotlandRegions("region_ons_code"),
        scotlandRegions("region_name")
      ).write.insertInto("d_geography")

    northEnglandAuthorities.join(northEnglandRegions,
      northEnglandAuthorities("region_ons_code") === northEnglandRegions("region_ons_code")).
      select(
        northEnglandAuthorities("local_authority_ons_code"),
        northEnglandAuthorities("local_authority_name"),
        northEnglandRegions("region_ons_code"),
        northEnglandRegions("region_name")
      ).write.insertInto("d_geography")

    southEnglandAuthorities.join(southEnglandRegions,
      southEnglandAuthorities("region_ons_code") === southEnglandRegions("region_ons_code")).
      select(
        southEnglandAuthorities("local_authority_ons_code"),
        southEnglandAuthorities("local_authority_name"),
        southEnglandRegions("region_ons_code"),
        southEnglandRegions("region_name")
      ).write.insertInto("d_geography")
  }
}
