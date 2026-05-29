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
    .appName("LAB11_Zadanie4_Okna") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

#Schemat i wczytywanie danych
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

#Konwersja czasu na właściwy typ Timestamp
df_cleaned = df.withColumn("event_time", to_timestamp(col("event_time")))

#Watermarking (Tolerancja na opóźnione dane)
#Dopuszczane dane spóźnione maksymalnie o 10 minut
df_watermarked = df_cleaned.withWatermark("event_time", "10 minutes")

#Agregacja w oknach czasowych

#Okno Stałe (Tumbling Window) - równe, niepokrywające się bloki 10-minutowe
tumbling_summary = df_watermarked \
    .groupBy(window(col("event_time"), "10 minutes"), col("category")) \
    .agg(count("*").alias("events_count"))

#Okno Przesuwne (Sliding Window) - bloki 10-minutowe, które generują się co 5 minut (nakładają się)
sliding_summary = df_watermarked \
    .groupBy(window(col("event_time"), "10 minutes", "5 minutes"), col("category")) \
    .agg(count("*").alias("events_count"))

#Uruchomienie strumieni do konsoli
#Użycie trybu update, ponieważ complete przy oknach i watermarkingu szybko zapchałby pamięć
print("Uruchamianie analizy dla okien stałych (Tumbling)...")
query_tumbling = tumbling_summary.writeStream \
    .format("console") \
    .outputMode("update") \
    .option("truncate", "false") \
    .start()

print("Uruchamianie analizy dla okien przesuwnych (Sliding)...")
query_sliding = sliding_summary.writeStream \
    .format("console") \
    .outputMode("update") \
    .option("truncate", "false") \
    .start()

#Oczekiwanie na oba strumienie
spark.streams.awaitAnyTermination()