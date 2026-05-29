import os
import sys
from pyspark.sql import SparkSession

#Konfiguracja środowiska Spark dla Windows
os.environ['HADOOP_HOME'] = "C:/hadoop"
os.environ['PATH'] = os.environ['PATH'] + ";C:/hadoop/bin"
os.environ['PYSPARK_SUBMIT_ARGS'] = '--driver-java-options "-Djava.security.manager=allow" pyspark-shell'

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

# Inicjalizacja sesji Spark
spark = SparkSession.builder \
    .appName("LAB11_StructuredStreaming") \
    .master("local[*]") \
    .getOrCreate()

print(f"Pomyślnie uruchomiono sesję Spark!")
print(f"Wersja Spark/PySpark: {spark.version}")

#Zakończenie sesji testowej
spark.stop()