from pyspark.sql import SparkSession
from pyspark.sql.functions import col, max, desc
from datetime import datetime

def perc(a, b):
  return ((a / b) * 100).astype("float")

spark = SparkSession \
    .builder \
    .appName("Dominance of constructors who won the championship each year") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")
spark.conf.set("spark.sql.shuffle.partitions", 10)

HDFSPath = "..."

racesHDFS = HDFSPath + "/races.csv"
constructor_standingsHDFS = HDFSPath + "/constructor_standings.csv" 
constructorsHDFS = HDFSPath + "/constructors.csv"

now = datetime.now()
current_time = now.strftime("%H:%M:%S")
print("Started on =", current_time)

races = spark.read.csv(racesHDFS, header = True, inferSchema = True)
constructor_standings = spark.read.csv(constructor_standingsHDFS, header = True, inferSchema = True)
constructors = spark.read.csv(constructorsHDFS, header = True, inferSchema = True)

races_year = races.groupby("year").agg(max("date"))
races_year = races_year.withColumnRenamed("max(date)", "lastRaceDate")
races_year = races_year.withColumnRenamed("year", "raceYear")

last_races = races.join(races_year, races.date == races_year.lastRaceDate)
last_races = last_races.select("raceId", "year", "date")

races_year = races.groupby("year").count()
races_year = races_year.withColumnRenamed("count", "numberRaces")

number_wins_of_winner = constructor_standings.join(last_races, constructor_standings.raceId == last_races.raceId)
number_wins_of_winner = number_wins_of_winner.join(constructors, number_wins_of_winner.constructorId == constructors.constructorId)
number_wins_of_winner = number_wins_of_winner.withColumnRenamed("year", "raceYear")
number_wins_of_winner = number_wins_of_winner.select("raceYear", "name", "wins").where((col("position") == 1))

number_wins_of_winner_total = number_wins_of_winner.join(races_year, number_wins_of_winner.raceYear == races_year.year)

dominance = number_wins_of_winner_total.withColumn("dominance_perc", perc(col("wins"), col("numberRaces")))
dominance.orderBy(desc("dominance_perc")).show()

now = datetime.now()
current_time = now.strftime("%H:%M:%S")
print("Ended on =", current_time)

spark.stop()