import os

os.environ["HADOOP_HOME"] = r"C:\hadoop"
os.environ["PATH"] = r"C:\hadoop\bin;" + os.environ["PATH"]

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    StringType,
    DoubleType
)

spark = (
    SparkSession.builder
    .appName("KafkaStreaming")
    .config("spark.hadoop.home.dir", r"C:\hadoop")
    .config(
        "spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0"
    )
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# Lecture Kafka
df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "ventes")
    .option("startingOffsets", "latest")
    .load()
)

# Schéma JSON
schema = StructType([
    StructField("id", IntegerType()),
    StructField("produit", StringType()),
    StructField("prix", DoubleType()),
    StructField("quantite", IntegerType())
])

# JSON → colonnes
df_ventes = (
    df.select(
        from_json(
            col("value").cast("string"),
            schema
        ).alias("data")
    )
    .select("data.*")
)

# Calcul du montant
df_resultat = df_ventes.withColumn(
    "montant",
    col("prix") * col("quantite")
)

# Affichage
query = (
    df_resultat
    .writeStream
    .format("parquet")
    .outputMode("append")
    .option(
        "path",
        r"C:\Users\lenovo\Documents\projet_kafka_pyspark\data\ventes"
    )
    .option(
        "checkpointLocation",
        r"C:\Users\lenovo\Documents\projet_kafka_pyspark\data\checkpoint"
    )
    .start()
)

query.awaitTermination()