package dbOperations

import org.apache.spark.sql.SparkSession

object dropTable {

  val spark = SparkSession.builder()
    .master("local[1]")
    .appName("SparkByExample")
    .getOrCreate()

  spark.sql("show tables").show()

  spark.sql("DROP TABLE IF EXISTS w_czas")
  spark.sql("DROP TABLE IF EXISTS w_pogoda")
  spark.sql("DROP TABLE IF EXISTS w_geografia")
  spark.sql("DROP TABLE IF EXISTS w_drogi")
  spark.sql("DROP TABLE IF EXISTS w_typ_pojazdu")
  spark.sql("DROP TABLE IF EXISTS f_fakty")


}
