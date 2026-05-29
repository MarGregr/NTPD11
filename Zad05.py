import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from pyspark.sql.functions import col, to_timestamp, count, window

#Konfiguracja środowiska Spark dla Windows
os.environ['HADOOP_HOME'] = "C:/hadoop"
os.environ['PATH'] = os.environ['PATH'] + ";C:/hadoop/bin"
os.environ['PYSPARK_SUBMIT_ARGS'] = '--driver-java-options "-Djava.security.manager=allow" pyspark-shell'

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

#Inicjalizacja sesji Spark
spark = SparkSession.builder \
    .appName("LAB11_Zadanie5_Checkpointing") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

#Schemat i wczytywanie strumienia
schema = StructType([
    StructField("event_time", StringType()),
    StructField("user_id", StringType()),
    StructField("category", StringType()),
    StructField("amount", DoubleType()),
    StructField("status", StringType()),
])

df = spark.readStream \
    .schema(schema) \
    .option("header", True) \
    .csv("data/input_stream")

df_cleaned = df.withColumn("event_time", to_timestamp(col("event_time")))

#Agregacja w oknach czasowych z watermarkiem
window_summary = df_cleaned.withWatermark("event_time", "10 minutes") \
    .groupBy(window(col("event_time"), "10 minutes"), col("category")) \
    .agg(count("*").alias("events_count"))

# WAŻNE: Spłaszczamy kolumnę 'window' (zawierającą start i end) do osobnych kolumn tekstowych/timestamp,
# aby format Parquet mógł je bez problemu zapisać.
flat_summary = window_summary.select(
    col("window.start").alias("window_start"),
    col("window.end").alias("window_end"),
    col("category"),
    col("events_count")
)

#Zapis strumieniowy z checkpointingiem
file_query = flat_summary.writeStream \
    .format("parquet") \
    .outputMode("append") \
    .option("path", "data/output_stream") \
    .option("checkpointLocation", "checkpoints/lab11") \
    .start()

print("Uruchomiono")
# print("Pozwól mu działać przez 15-20 sekund (niech generator doda pliki),")
# print("a następnie wyłącz program ręcznie wciskając Ctrl + C.")


try:
    file_query.awaitTermination()
except KeyboardInterrupt:
    print("\nRęcznie zatrzymano strumień. Zamykanie zapytania...")
    file_query.stop()


spark.stop()