from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when
from datetime import datetime

spark = SparkSession \
    .builder \
    .appName("Number of race wins") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")
spark.conf.set("spark.sql.shuffle.partitions", 10)

HDFSPath = "..."

resultsHDFS = HDFSPath + "/results.csv"
driversHDFS = HDFSPath + "/drivers.csv" 

now = datetime.now()
current_time = now.strftime("%H:%M:%S")
print("Started on =", current_time)

results = spark.read.csv(resultsHDFS, header=True, inferSchema=True)
results = results.select("driverId", "positionOrder")
results = results.where(results.positionOrder == 1)

winners = results.groupby("driverId").count()
winners = winners.withColumnRenamed("count", "wins")

drivers = spark.read.csv(driversHDFS, header=True, inferSchema=True)
drivers = drivers.select("driverId", "forename", "surname")

driver_res = winners.join(drivers, winners.driverId == drivers.driverId, how="outer")
driver_res = driver_res.select("forename", "surname", "wins")
driver_res = driver_res.withColumn("wins", when(col("wins").isNotNull(), col("wins")).otherwise(0))
driver_res = driver_res.sort("wins", ascending=False)
driver_res.show(1000)

now = datetime.now()
current_time = now.strftime("%H:%M:%S")
print("Ended on =", current_time)

spark.stop()