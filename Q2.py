from pyspark.sql import SparkSession
from pyspark.sql.functions import col, split, regexp_replace, when, least
from datetime import datetime

NO_TIME = 999999999

def time_to_ms(min_sec_ms):
    min_sec_ms = regexp_replace(min_sec_ms, "\\.", ":")
    min__sec__ms = split(min_sec_ms, ":")
    min = min__sec__ms.getItem(0).cast("int")
    sec = min__sec__ms.getItem(1).cast("int")
    ms = min__sec__ms.getItem(2).cast("int")
    return min * 60 * 1000 + sec * 1000 + ms

spark = SparkSession \
    .builder \
    .appName("Average percentage difference between race (best, average and worst) vs qualifying lap times") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")
spark.conf.set("spark.sql.shuffle.partitions", 10)

HDFSPath = "..."

qualiHDFS = HDFSPath + "/qualifying.csv"
laptimesHDFS = HDFSPath + "/lap_times.csv" 
resultsHDFS = HDFSPath + "/results.csv"

now = datetime.now()
current_time = now.strftime("%H:%M:%S")
print("Started on =", current_time)

quali = spark.read.csv(qualiHDFS, header=True, inferSchema=True)
quali = quali.select("raceId", "driverId", "q1", "q2", "q3")
quali = quali.withColumn("q1_ms", when(col("q1") != "\\N", time_to_ms(col("q1"))).otherwise(NO_TIME))
quali = quali.withColumn("q2_ms", when(col("q2") != "\\N", time_to_ms(col("q2"))).otherwise(NO_TIME))
quali = quali.withColumn("q3_ms", when(col("q3") != "\\N", time_to_ms(col("q3"))).otherwise(NO_TIME))
quali = quali.withColumn("best_quali", least(col("q1_ms"), col("q2_ms"), col("q3_ms")))
quali = quali.where(col("best_quali") != NO_TIME)
quali = quali.select("raceId", "driverId", "best_quali")

results = spark.read.csv(resultsHDFS, header=True, inferSchema=True)
results = results.select("raceId", "driverId", "statusId")
finished_results = results.where(col("statusId") == 1)
finished_results = finished_results.select("raceId", "driverId")

racelaps = spark.read.csv(laptimesHDFS, header=True, inferSchema=True)
racelaps = racelaps.select("raceId", "driverId", "milliseconds")
racelaps = racelaps.join(finished_results, ["raceId", "driverId"], how="inner")

driver_race_laps = racelaps.groupby("raceId", "driverId")

avg_laps = driver_race_laps.avg("milliseconds") \
    .withColumnRenamed("avg(milliseconds)", "avg_lap")
best_laps = driver_race_laps.min("milliseconds") \
    .withColumnRenamed("min(milliseconds)", "best_lap")
worst_laps = driver_race_laps.max("milliseconds") \
    .withColumnRenamed("max(milliseconds)", "worst_lap")

race_quali = quali.join(avg_laps, ["raceId", "driverId"], how="inner") \
    .join(best_laps, ["raceId", "driverId"], how="inner") \
    .join(worst_laps, ["raceId", "driverId"], how="inner")

race_quali = race_quali \
    .withColumn("perc_diff_best", (col("best_lap") - col("best_quali")) / col("best_lap") * 100) \
    .withColumn("perc_diff_avg", (col("avg_lap") - col("best_quali")) / col("avg_lap") * 100) \
    .withColumn("perc_diff_worst", (col("worst_lap") - col("best_quali")) / col("worst_lap") * 100)

race_quali.select("perc_diff_best", "perc_diff_avg", "perc_diff_worst").summary().show()

#Extra, interesting results
driver = spark.read.csv(HDFSPath + "/drivers.csv", header=True, inferSchema=True)
races = spark.read.csv(HDFSPath + "/races.csv", header=True, inferSchema=True)
driver = driver.select("driverId", "forename", "surname")
races = races.select("raceId", "year", "name")
detailed_comparison = race_quali.join(driver, "driverId", how="inner") \
    .join(races, "raceId", how="inner") \
    .select("forename", "surname", "year", "name", "perc_diff_best", "perc_diff_avg", "perc_diff_worst", "best_lap", "avg_lap", "worst_lap", "best_quali")

print("Best best lap compared to quali")
detailed_comparison.sort("perc_diff_best", ascending=True).show(1)
print("Worst best lap compared to quali")
detailed_comparison.sort("perc_diff_best", ascending=False).show(1)
print("Best avg lap compared to quali")
detailed_comparison.sort("perc_diff_avg", ascending=True).show(1)
print("Worst avg lap compared to quali")
detailed_comparison.sort("perc_diff_avg", ascending=False).show(1)
print("Best worst lap compared to quali")
detailed_comparison.sort("perc_diff_worst", ascending=True).show(1)
print("Worst worst lap compared to quali")
detailed_comparison.sort("perc_diff_worst", ascending=False).show(1)

now = datetime.now()
current_time = now.strftime("%H:%M:%S")
print("Ended on =", current_time)

spark.stop()