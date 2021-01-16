import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{monotonically_increasing_id}

object WeatherTransformation {

  val spark = SparkSession.builder()
    .appName("WeatherTransformation")
    .getOrCreate()

  import spark.implicits._

  case class WeatherFromFile(region: String, date: String, time: String, conditions: String)

  def main(args: Array[String]): Unit = {
    val username = "adepcio"

    val weatherFile = spark.sparkContext.textFile(s"/user/$username/proj/spark/weather.txt")
    val linesRdd = weatherFile.flatMap(_.split("\n"))

    val capturePattern =
      """In the region of ([A-Z0-9]+|null) on ([0-9]{2}\/[0-9]{2}\/[0-9]{4}|null) at ([0-9:]+|null) the following weather conditions were reported: ([A-Za-z ]+|null)""".r

    val matches = linesRdd.map(line => {
      val capturePattern(region, date, time, conditions) = line
      WeatherFromFile(region, date, time, conditions)
    })

    val matchesDS = matches.toDS
    val matchesNoDuplicates = matchesDS.dropDuplicates("conditions")

    matchesNoDuplicates.withColumn("index", monotonically_increasing_id()).
      select($"index", $"conditions").
      write.insertInto("w_pogoda")
  }
}
