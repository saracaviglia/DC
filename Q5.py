from pyspark.sql import SparkSession
from pyspark.sql.functions import desc
from datetime import datetime

spark = SparkSession \
    .builder \
    .appName("Most raced circuits") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")
spark.conf.set("spark.sql.shuffle.partitions", 10)

HDFSPath = "..."

circuitsHDFS = HDFSPath + "/circuits.csv"
racesHDFS = HDFSPath + "/races.csv"

now = datetime.now()
current_time = now.strftime("%H:%M:%S")
print("Started on =", current_time)

circuits = spark.read.csv(circuitsHDFS, header = True, inferSchema = True)
races = spark.read.csv(racesHDFS, header = True, inferSchema = True)

most_raced_id = races.groupBy("circuitId").count().orderBy(desc("count"))
most_raced_id = most_raced_id.withColumnRenamed("count", "numberRaces")

circuits = circuits.withColumnRenamed("circuitId", "id")
most_raced_circuits = circuits.join(most_raced_id, circuits.id == most_raced_id.circuitId, "inner")
most_raced_circuits = most_raced_circuits.select("circuitId", "name", "numberRaces").orderBy(desc("numberRaces"))

most_raced_circuits.show(100)

now = datetime.now()
current_time = now.strftime("%H:%M:%S")
print("Ended on =", current_time)

spark.stop()
