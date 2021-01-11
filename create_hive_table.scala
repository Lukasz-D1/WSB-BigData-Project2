spark.sql("show tables").show() 

spark.sql("""CREATE TABLE `w_czas` ( 
    `data` date, 
    `dzien` int,
    `miesiac` int, 
    `rok` int,
    `godzina` int,) 
ROW FORMAT SERDE 
    'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
STORED AS INPUTFORMAT
    'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
 OUTPUTFORMAT 
    'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'""")

spark.sql("""CREATE TABLE `w_pogoda` ( 
    `id_pogody` int, 
    `warunki_pogodowe` string) 
ROW FORMAT SERDE 
    'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
STORED AS INPUTFORMAT
    'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
 OUTPUTFORMAT 
    'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'""")

spark.sql("""CREATE TABLE `w_geografia` ( 
    `id_jednostki_adm` string, 
    `nazwa` string,
    `kod_regionu` string,
    `nazwa_regionu` string) 
ROW FORMAT SERDE 
    'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
STORED AS INPUTFORMAT
    'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
 OUTPUTFORMAT 
    'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'""")

spark.sql("""CREATE TABLE `w_drogi` ( 
    `id_drogi` int, 
    `typ_drogi` string,
    `kategoria_drogi` string) 
ROW FORMAT SERDE 
    'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
STORED AS INPUTFORMAT
    'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
 OUTPUTFORMAT 
    'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'""")

spark.sql("""CREATE TABLE `w_typ_pojazdu` ( 
    `id_typu_pojazdu` int, 
    `typ_pojazdu` string,
    `kategoria_pojazdu` string,
    `czy_silnik` boolean) 
ROW FORMAT SERDE 
    'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
STORED AS INPUTFORMAT
    'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
 OUTPUTFORMAT 
    'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'""")

spark.sql("show tables").show() 
