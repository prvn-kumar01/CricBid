from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from db_config import jdbc_url, properties

# Spark Session Start
spark = SparkSession.builder \
    .appName("CricBid Deliveries EDA") \
    .config("spark.jars", "D:\\postgresql-42.7.5.jar") \
    .getOrCreate()

# Load Cleaned CSV
df = spark.read.option("header", "true").option("inferSchema", "true") \
    .csv("Datasets/deliveries_cl.csv")

# 1. Team-wise Total Runs Scored
team_runs = df.groupBy("batting_team") \
    .sum("total_runs") \
    .withColumnRenamed("sum(total_runs)", "total_runs_scored")

# 2. Top 10 Batsmen by Total Runs
top_batsmen = df.groupBy("batter") \
    .sum("batsman_runs") \
    .withColumnRenamed("sum(batsman_runs)", "total_runs") \
    .orderBy(col("total_runs").desc()) \
    .limit(10)

# 3. Top 10 Bowlers by Wickets
top_bowlers = df.filter(col("dismissal_kind").isNotNull()) \
    .groupBy("bowler") \
    .count() \
    .withColumnRenamed("count", "wickets") \
    .orderBy(col("wickets").desc()) \
    .limit(10)
# 4. 

# PostgreSQL Upload
team_runs.write.jdbc(url=jdbc_url, table="team_total_runs", mode="overwrite", properties=properties)
top_batsmen.write.jdbc(url=jdbc_url, table="top_batsmen", mode="overwrite", properties=properties)
top_bowlers.write.jdbc(url=jdbc_url, table="top_bowlers", mode="overwrite", properties=properties)

print("EDA results successfully written to PostgreSQL!")
