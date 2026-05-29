import os
import sys
from pyspark.sql import SparkSession

# Konfiguracja środowiska Spark dla Windows
os.environ['HADOOP_HOME'] = "C:/hadoop"
os.environ['PATH'] = os.environ['PATH'] + ";C:/hadoop/bin"
os.environ['PYSPARK_SUBMIT_ARGS'] = '--driver-java-options "-Djava.security.manager=allow" pyspark-shell'

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

# Inicjalizacja tradycyjnej (statycznej) sesji Spark dla przetwarzania Batch
spark = SparkSession.builder \
    .appName("LAB11_Zadanie5_Weryfikacja_Batch") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print("Uruchamianie odczytu:")

try:
    # Wskazanie "*.parquet" zmusza Sparka do zaczytania samych plików z pominięciem usterki w metadanych
    batch_df = spark.read.parquet("data/output_stream/*.parquet")

    print(f"\n[SUKCES] Liczba zapisanych wierszy w plikach Parquet: {batch_df.count()}")
    print("Aktualna zawartość zapisana na dysku przez strumień:")
    batch_df.show(truncate=False)
except Exception as e:
    print(f"\n[BŁĄD] Nie można odczytać danych.")
    print(f"Powód: {e}")

spark.stop()