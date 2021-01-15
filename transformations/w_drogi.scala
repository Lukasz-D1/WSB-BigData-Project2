import org.apache.spark.sql._
import org.apache.spark.sql.expressions.Window

// change username
val username = "username"

spark.sql("DROP TABLE IF EXISTS w_drogi")
spark.sql(
  """CREATE TABLE IF NOT EXISTS `w_drogi` (
    `id` int,
    `road_category` string,
    `road_type` string)
      ROW FORMAT SERDE
    'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
      STORED AS INPUTFORMAT
    'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
      OUTPUTFORMAT
    'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'"""
)

val northEnglandRoads = spark.read.
  format("org.apache.spark.csv").
  option("header", true).
  option("inferSchema", true).
  csv(s"/user/$username/proj/spark/mainDataNorthEngland.csv").cache()

val northEnglandRoadsWithNoIds = northEnglandRoads
  .select(
    "road_category",
    "road_type"
  ).dropDuplicates()

val windowSortByRoadCategory = Window.orderBy("road_category")

northEnglandRoadsWithNoIds.withColumn("id", row_number().over(windowSortByRoadCategory))
  .select(
    "id",
    "road_category",
    "road_type"
  )
  .write
  .insertInto("w_drogi")

val drogi = spark.sql("SELECT id, road_category, road_type FROM w_drogi")

drogi.show()