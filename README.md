# parquet
# Να γραφτεί κώδικας που θα διαβάσει τα αρχεία δεδομένων και με την κατάλληλη επεξεργασία θα τα αποθηκεύσει σε μορφή parquet στο HDFS, στο παρακάτω path:  hdfs://hdfs-namenode:9000/user/{username}/data/parquet/ 

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("CSV_to_Parquet").getOrCreate()

files = [
    ("/data/yellow_tripdata_2015.csv", "/user/spetroutsatos/data/parquet/yellow_tripdata_2015"),
    ("/data/yellow_tripdata_2024.csv", "/user/spetroutsatos/data/parquet/yellow_tripdata_2024"),
    ("/data/taxi_zone_lookup.csv",     "/user/spetroutsatos/data/parquet/taxi_zone_lookup")
]

for csv_path, parquet_path in files:
    print(f"Μετατροπή: {csv_path} -> {parquet_path}")
    df = spark.read.option("header", "true").csv(f"hdfs://hdfs-namenode:9000{csv_path}")
    df.write.mode("overwrite").parquet(f"hdfs://hdfs-namenode:9000{parquet_path}")
