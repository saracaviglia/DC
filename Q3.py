from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from datetime import datetime

spark = SparkSession \
    .builder \
    .appName("Fastest lap for each circuit") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")
spark.conf.set("spark.sql.shuffle.partitions", 10)

HDFSPath = "..."

racesHDFS = HDFSPath + "/races.csv"
driversHDFS = HDFSPath + "/drivers.csv"
lap_timesHDFS = HDFSPath + "/lap_times.csv"

now = datetime.now()
current_time = now.strftime("%H:%M:%S")
print("Started on =", current_time)

races = spark.read.csv(racesHDFS, header = True, inferSchema = True)
drivers = spark.read.csv(driversHDFS, header = True, inferSchema = True)
lap_times = spark.read.csv(lap_timesHDFS, header = True, inferSchema = True)

races = races.select("raceId", "year", "name")
drivers = drivers.select("driverId", "forename", "surname")
lap_times = lap_times.select("raceId", "driverId", "lap", "milliseconds")

lap_times = lap_times.withColumn("milliseconds", col("milliseconds").cast("int"))

res = lap_times.join(races, on = "raceId")
res = res.join(drivers, on = "driverId")

res = res.withColumnRenamed("name", "race_name")
res = res.withColumnRenamed("forename", "driver_forename")
res = res.withColumnRenamed("surname", "driver_surname")

res = res.select("race_name", "milliseconds", "driver_forename", "driver_surname")
res_race = res.groupby("race_name").min("milliseconds")
res_race = res_race.withColumnRenamed("min(milliseconds)", "milliseconds")
res_race = res_race.join(res, on = ["race_name", "milliseconds"], how = "inner")
res_race = res_race.select("race_name", "milliseconds", "driver_forename", "driver_surname")

res_race.show(100)

now = datetime.now()
current_time = now.strftime("%H:%M:%S")
print("Ended on =", current_time)

spark.stop()