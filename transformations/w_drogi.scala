import org.apache.spark.sql._

// change username
val username = "username"

val scotlandRoads = spark.read.
  format("org.apache.spark.csv").
  option("header", true).
  option("inferSchema", true).
  csv(s"/user/$username/proj/spark/mainDataScotland.csv").cache()

scotlandRoads.select(
  "road_name",
  "road_category",
  "road_type"
).write.insertInto("w_drogi")

val drogi = spark.sql("SELECT id_jednostki_adm, nazwa FROM w_drogi")

drogi.show()