import os
import sys
import time
import random
from datetime import datetime

#Konfiguracja środowiska Spark dla Windows
os.environ['HADOOP_HOME'] = "C:/hadoop"
os.environ['PATH'] = os.environ['PATH'] + ";C:/hadoop/bin"
os.environ['PYSPARK_SUBMIT_ARGS'] = '--driver-java-options "-Djava.security.manager=allow" pyspark-shell'

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

#Tworzenie folderu wejściowego, jeśli nie istnieje
INPUT_DIR = "data/input_stream"
os.makedirs(INPUT_DIR, exist_ok=True)

categories = ["books", "electronics", "clothing", "home", "sports"]
statuses = ["paid", "pending", "cancelled"]

print(f"Uruchomiono generator. Pliki CSV będą zapisywane w: {INPUT_DIR}")
print("Naciśnij Ctrl+C, aby przerwać.")

file_counter = 1

try:
    while True:
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        filename = f"{INPUT_DIR}/events_{file_counter}_{int(time.time())}.csv"

        with open(filename, "w", encoding="utf-8") as f:
            f.write("event_time,user_id,category,amount,status\n")
            #Generowanie 3-5 losowych rekordów w jednym pliku
            for _ in range(random.randint(3, 5)):
                user_id = f"u{random.randint(100, 999)}"
                category = random.choice(categories)
                amount = round(random.uniform(10.0, 500.0), 2)
                status = random.choice(statuses)
                f.write(f"{timestamp},{user_id},{category},{amount},{status}\n")

        print(f"[{datetime.now().strftime('%H:%M:%S')}] Wygenerowano plik: {filename}")
        file_counter += 1
        time.sleep(20)  # Nowy plik co 20 sekund
except KeyboardInterrupt:
    print("\nZakończono generowanie danych.")