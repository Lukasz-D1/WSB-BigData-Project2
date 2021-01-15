import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


val spark = SparkSession.builder()
  .master("local[1]")
  .appName("SparkByExample")
  .getOrCreate()

import spark.implicits._


// change username
val username = "marcin8768"

spark.sql("DROP TABLE IF EXISTS w_czas")

spark.sql(
  """CREATE TABLE IF NOT EXISTS `w_czas` (
    `id` bigint,
    `data` string,
    `rok` int,
    `miesiac` int,
    `dzien` int,
    `godzina` int)
      ROW FORMAT SERDE
    'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
      STORED AS INPUTFORMAT
    'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
      OUTPUTFORMAT
    'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'""")

val mainDataNorthEngland = spark.read.
  format("com.databricks.spark.csv").
  option("header", true).
  option("inferSchema", true).
  csv(s"/user/$username/proj/spark/mainDataNorthEngland.csv").cache()

val mainDataScotland = spark.read.
  format("com.databricks.spark.csv").
  option("header", true).
  option("inferSchema", true).
  csv(s"/user/$username/proj/spark/mainDataScotland.csv").cache()

val mainDataSouthEngland = spark.read.
  format("com.databricks.spark.csv").
  option("header", true).
  option("inferSchema", true).
  csv(s"/user/$username/proj/spark/mainDataSouthEngland.csv").cache()




case class DateObject (
                        id: BigInt,
                        date: String,
                        year: String,
                        month: String,
                        day: String,
                        hour: Double
                      )


val tablica1 = mainDataNorthEngland.select(mainDataNorthEngland("count_date"),mainDataNorthEngland("hour")).
  union(mainDataScotland.select(mainDataScotland("count_date"),mainDataScotland("hour")).
    union(mainDataSouthEngland.select(mainDataSouthEngland("count_date"),mainDataSouthEngland("hour") ))).
  distinct()





val time2 = tablica1.select(
  tablica1("count_date"), tablica1("hour")
).
  withColumn("_tmp", date_format(col("count_date"),"yyyy-MM-dd")).
  withColumn("_tmp2", split($"_tmp", "\\-")).
  withColumn("UniqueID", monotonically_increasing_id).
  select(
    $"UniqueID".as("id"),
    $"_tmp".as("date"),
    $"_tmp2".getItem(0).as("year"),
    $"_tmp2".getItem(1).as("month"),
    $"_tmp2".getItem(2).as("day"),
    $"hour").toDF().as[DateObject]

time2.write.insertInto("w_czas")


// val czas1 = spark.sql("SELECT * FROM w_czas")
// czas1.show
// czas1.count()
