package com.wsb.project

import org.apache.spark.sql._
import org.apache.spark.sql.functions.{col, date_format, dayofmonth, hour, month, unix_timestamp, year}
import org.apache.spark.sql.types._


object TimeTransformation {
  val spark: SparkSession = SparkSession.builder()
    .appName("timeTransformation")
    .getOrCreate()

  def readCsv(path: String): DataFrame = {
    spark.read.
      format("org.apache.spark.csv").
      option("header", value = true).
      option("inferSchema", value = true).
      csv(path)
  }

  case class datetime(year: Int,
                      month: Int,
                      day: Int,
                      hour: Int)

  val username = "username"

  val mainDataNorthEngland : String = s"/user/$username/proj/spark/mainDataNorthEngland.csv"
  val mainDataScotland :String = s"/user/$username/proj/spark/mainDataScotland.csv"
  val mainDataSouthEngland :String = s"/user/$username/proj/spark/mainDataSouthEngland.csv"


  def main(args: Array[String]): Unit = {
    val mainDataNorthEngland_df : DataFrame = readCsv(mainDataNorthEngland).cache()
    val mainDataScotland_df : DataFrame = readCsv(mainDataScotland).cache()
    val mainDataSouthEngland_df : DataFrame = readCsv(mainDataSouthEngland).cache()

    val dataUnion = mainDataNorthEngland_df.select(mainDataNorthEngland_df("count_date"), mainDataNorthEngland_df("hour")).
      union(mainDataScotland_df.select(mainDataScotland_df("count_date"), mainDataScotland_df("hour")).
        union(mainDataSouthEngland_df.select(mainDataSouthEngland_df("count_date"), mainDataSouthEngland_df("hour")))).
      distinct()

    val timetowrite = dataUnion.withColumn("timestamp", (unix_timestamp(date_format(col("count_date"),"yyyy-MM-dd") , "yyyy-MM-dd").as("timestamp")+ $"hour" * 60 * 60).cast(TimestampType))
      .select(
        col("timestamp"),
        year(col("timestamp")).alias("year"),
        month(col("timestamp")).alias("month"),
        dayofmonth(col("timestamp")).alias("day"),
        hour(col("timestamp")).alias("hour")
      ) : DataFrame

    timetowrite.write.insertInto("d_time")

  }

}
