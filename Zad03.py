import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from pyspark.sql.functions import col, to_timestamp, count, sum, round

#Konfiguracja środowiska Spark dla Windows
os.environ['HADOOP_HOME'] = "C:/hadoop"
os.environ['PATH'] = os.environ['PATH'] + ";C:/hadoop/bin"
os.environ['PYSPARK_SUBMIT_ARGS'] = '--driver-java-options "-Djava.security.manager=allow" pyspark-shell'

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

#Inicjalizacja sesji Spark
spark = SparkSession.builder \
    .appName("LAB11_Zadanie3_Agregacje") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

#Definicja schematu
schema = StructType([
    StructField("event_time", StringType()),
    StructField("user_id", StringType()),
    StructField("category", StringType()),
    StructField("amount", DoubleType()),
    StructField("status", StringType()),
])

#Wczytywanie strumienia
df = spark.readStream \
    .schema(schema) \
    .option("header", True) \
    .csv("data/input_stream")

#Zastosowane trzy transformacje
# Konwersja czasu ze String na Timestamp
# Filtrowanie - bierzemy pod uwagę tylko opłacone zamówienia (status == "paid")
# Dodanie nowej kolumny - obliczenie szacowanego podatku VAT (23%) zaokrąglonego do 2 miejsc
df_transformed = df \
    .withColumn("event_time", to_timestamp(col("event_time"))) \
    .filter(col("status") == "paid") \
    .withColumn("estimated_vat", round(col("amount") * 0.23, 2))

#Grupowanie po kategorii, zliczanie wystąpień oraz suma kwot i wyliczonego VAT-u
summary = df_transformed.groupBy("category") \
    .agg(
        count("*").alias("events_count"),
        round(sum("amount"), 2).alias("total_amount"),
        round(sum("estimated_vat"), 2).alias("total_vat")
    )

#Uruchomienie strumienia
#Wybór trybu "complete" pozwala na bieżąco nadpisywać tabelę w konsoli nowymi sumami
query = summary.writeStream \
    .format("console") \
    .outputMode("complete") \
    .start()

#Uruchomienie nasłuchiwania
query.awaitTermination()