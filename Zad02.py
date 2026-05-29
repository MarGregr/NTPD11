import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from pyspark.sql.functions import col, to_timestamp

#Konfiguracja środowiska Spark dla Windows
os.environ['HADOOP_HOME'] = "C:/hadoop"
os.environ['PATH'] = os.environ['PATH'] + ";C:/hadoop/bin"
os.environ['PYSPARK_SUBMIT_ARGS'] = '--driver-java-options "-Djava.security.manager=allow" pyspark-shell'

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

#Inicjalizacja sesji Spark
spark = SparkSession.builder \
    .appName("LAB11_Zadanie2_Streaming") \
    .master("local[*]") \
    .getOrCreate()

#Ustawienie poziomu logowania na WARN, żeby logi Sparka nie zasypały konsoli
spark.sparkContext.setLogLevel("WARN")

#Definicja schematu danych wejściowych
schema = StructType([
    StructField("event_time", StringType()),
    StructField("user_id", StringType()),
    StructField("category", StringType()),
    StructField("amount", DoubleType()),
    StructField("status", StringType()),
])

#Strumieniowe wczytywanie danych (readStream) z folderu wejściowego
df = spark.readStream \
    .schema(schema) \
    .option("header", True) \
    .csv("data/input_stream")

#odstawowe czyszczenie i transformacja danych:
#Konwersja czasu ze StringType na TimestampType
#Odrzucenie rekordów, gdzie kwota (amount) jest mniejsza lub równa 0 (czyszczenie błędów)
df_cleaned = df \
    .withColumn("event_time", to_timestamp(col("event_time"))) \
    .filter(col("amount") > 0)

#Weryfikacja: Sprawdzenie, czy DataFrame jest strumieniowy
is_streaming = df_cleaned.isStreaming
print(f"Czy dataframe jest strumieniowy?: {is_streaming}")

#Wyświetlenie schematu w konsoli
print("Schemat przetworzonego DataFrame:")
df_cleaned.printSchema()

#Na potrzeby samego Zadania 2, aby zobaczyć czy strumień rusza,
# można na chwilę odpalić najprostszy zapis do konsoli.
query = df_cleaned.writeStream \
    .format("console") \
    .outputMode("append") \
    .start()

#Oczekiwanie na zakończenie (zatrzymaj proces ręcznie przez Ctrl+C)
query.awaitTermination()