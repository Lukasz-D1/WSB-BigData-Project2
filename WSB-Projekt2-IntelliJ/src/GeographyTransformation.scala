package com.wsb.project

import org.apache.spark.sql.SparkSession

object GeographyTransformation {

  val spark = SparkSession.builder()
    .appName("GeographyTransformation")
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

    val scotlandAuthoritiesPath = s"/user/$username/proj/spark/authoritiesScotland.csv"
    val scotlandRegionsPath = s"/user/$username/proj/spark/regionsScotland.csv"

    val northEnglandAuthoritiesPath = s"/user/$username/proj/spark/authoritiesNorthEngland.csv"
    val northEnglandRegionsPath = s"/user/$username/proj/spark/regionsNorthEngland.csv"

    val southEnglandAuthoritiesPath = s"/user/$username/proj/spark/authoritiesSouthEngland.csv"
    val southEnglandRegionsPath = s"/user/$username/proj/spark/regionsSouthEngland.csv"

    val scotlandAuthorities = readCsv(scotlandAuthoritiesPath)

    val scotlandRegions = readCsv(scotlandRegionsPath)

    val northEnglandAuthorities = readCsv(northEnglandAuthoritiesPath)

    val northEnglandRegions = readCsv(northEnglandRegionsPath)

    val southEnglandAuthorities = readCsv(southEnglandAuthoritiesPath)

    val southEnglandRegions = readCsv(southEnglandRegionsPath)

    scotlandAuthorities.join(scotlandRegions,
      scotlandAuthorities("region_ons_code") === scotlandRegions("region_ons_code")).
      select(
        scotlandAuthorities("local_authority_ons_code"),
        scotlandAuthorities("local_authority_name"),
        scotlandRegions("region_ons_code"),
        scotlandRegions("region_name")
      ).write.insertInto("w_geografia")

    northEnglandAuthorities.join(northEnglandRegions,
      northEnglandAuthorities("region_ons_code") === northEnglandRegions("region_ons_code")).
      select(
        northEnglandAuthorities("local_authority_ons_code"),
        northEnglandAuthorities("local_authority_name"),
        northEnglandRegions("region_ons_code"),
        northEnglandRegions("region_name")
      ).write.insertInto("w_geografia")

    southEnglandAuthorities.join(southEnglandRegions,
      southEnglandAuthorities("region_ons_code") === southEnglandRegions("region_ons_code")).
      select(
        southEnglandAuthorities("local_authority_ons_code"),
        southEnglandAuthorities("local_authority_name"),
        southEnglandRegions("region_ons_code"),
        southEnglandRegions("region_name")
      ).write.insertInto("w_geografia")
  }
}
