import org.apache.spark.sql.SparkSession

val spark = SparkSession.builder()
  .master("local[1]")
  .appName("SparkByExample")
  .getOrCreate()

// change username
val username = "marcin8768"

spark.sql("DROP TABLE IF EXISTS w_typ_pojazdu")

spark.sql(
  """CREATE TABLE IF NOT EXISTS `w_typ_pojazdu` (
    `id` bigint,
    `vehicle_type` string,
    `vehicle_category` string,
    `has_engine` boolean
    )
      ROW FORMAT SERDE
    'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
      STORED AS INPUTFORMAT
    'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
      OUTPUTFORMAT
    'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'""")



spark.sql("""INSERT INTO w_typ_pojazdu VALUES
  ('1','pedal_cycles','pedal_cycles','false'),
  ('2','two_wheeled_motor_vehicles','two_wheeled_motor_vehicles','true'),
  ('3','cars_and_taxis','cars_and_taxis','true'),
  ('4','buses_and_coaches','buses_and_coaches','true'),
  ('5','lgvs','lgvs','true'),
  ('6','hgvs_2_rigid_axle','hgvs','true'),
  ('7','hgvs_3_rigid_axle','hgvs','true'),
  ('8','hgvs_4_or_more_rigid_axle','hgvs','true'),
  ('9','hgvs_3_or_4_articulated_axle','hgvs','true'),
  ('10','hgvs_5_articulated_axle','hgvs','true'),
  ('11','hgvs_6_articulated_axle','hgvs','true')
"""
)


val typ_pojazdu = spark.sql("SELECT * FROM w_typ_pojazdu")

typ_pojazdu.show
