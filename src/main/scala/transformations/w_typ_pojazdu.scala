import org.apache.spark.sql.SparkSession


val spark = SparkSession.builder()
  .master("local[1]")
  .appName("SparkByExample")
  .getOrCreate()

import spark.implicits._

// change username
val username = "marcin8768"



spark.sql("DROP TABLE IF EXISTS w_typ_pojazdu")

case class Vehicle (
                     id: BigInt,
                     vehicle_type: String,
                     vehicle_category: String,
                     has_engine: Boolean
                   )


val vehicles =
  Seq(Vehicle(1,"pedal_cycles","pedal_cycles",false),
    Vehicle(2,"two_wheeled_motor_vehicles","two_wheeled_motor_vehicles",true),
    Vehicle(3,"cars_and_taxis","cars_and_taxis",true),
    Vehicle(4,"buses_and_coaches","buses_and_coaches",true),
    Vehicle(5,"lgvs","lgvs",true),
    Vehicle(6,"hgvs_2_rigid_axle","hgvs",true),
    Vehicle(7,"hgvs_3_rigid_axle","hgvs",true),
    Vehicle(8,"hgvs_4_or_more_rigid_axle","hgvs",true),
    Vehicle(9,"hgvs_3_or_4_articulated_axle","hgvs",true),
    Vehicle(10,"hgvs_5_articulated_axle","hgvs",true),
    Vehicle(11,"hgvs_6_articulated_axle","hgvs",true)
  ).toDS.write.format("orc").saveAsTable("w_typ_pojazdu")


//todo do usunięcia
val pojazdy_z_bazy = spark.sql("SELECT * FROM w_typ_pojazdu")
pojazdy_z_bazy .show
pojazdy_z_bazy .count()

