from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import requests
from datetime import datetime

KAFKA_BROKER = "kafka:19092"
KAFKA_TOPIC = "trades"
INFLUXDB_URL = "http://influxdb:8086/api/v2/write"
INFLUXDB_TOKEN = "trading-token-123"
INFLUXDB_ORG = "trading"
INFLUXDB_BUCKET = "market_data"

def write_to_influxdb(df, epoch_id):
    """Escribir cada batch a InfluxDB"""
    if df.count() > 0:
        rows = df.collect()
        for row in rows:
            # Crear línea de datos en formato InfluxDB
            influx_line = f"ohlc,symbol={row['symbol']} " \
                          f"open={row['open']},high={row['high']}," \
                          f"low={row['low']},close={row['close']}," \
                          f"volume={row['volume']},trade_count={row['trade_count']} " \
                          f"{int(row['window_start'].timestamp()) * 1000000000}"
            
            try:
                response = requests.post(
                    INFLUXDB_URL,
                    headers={
                        "Authorization": f"Token {INFLUXDB_TOKEN}",
                        "Content-Type": "text/plain"
                    },
                    params={
                        "org": INFLUXDB_ORG,
                        "bucket": INFLUXDB_BUCKET,
                        "precision": "ns"
                    },
                    data=influx_line
                )
                if response.status_code != 204:
                    print(f"❌ Error escribiendo a InfluxDB: {response.text}")
                else:
                    print(f"✅ Vela guardada: {row['symbol']} - {row['window_start']}")
            except Exception as e:
                print(f"❌ Error: {e}")

def run_spark_streaming():
    print("🚀 Iniciando sesión de Spark...")
    
    spark = SparkSession.builder \
        .appName("TradingOHLCProcessor") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1") \
        .config("spark.sql.session.timeZone", "UTC") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    
    # Definir esquema de los trades
    trade_schema = StructType([
        StructField("symbol", StringType(), True),
        StructField("price", DoubleType(), True),
        StructField("volume", DoubleType(), True),
        StructField("timestamp", LongType(), True)
    ])
    
    print(f"📡 Conectando a Kafka en {KAFKA_BROKER}, topic '{KAFKA_TOPIC}'...")
    
    # Leer stream de Kafka
    kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKER) \
        .option("subscribe", KAFKA_TOPIC) \
        .option("startingOffsets", "latest") \
        .load()
    
    # Parsear JSON
    parsed_df = kafka_df \
        .selectExpr("CAST(value AS STRING) as json_string", "timestamp") \
        .select(from_json(col("json_string"), trade_schema).alias("data"), "timestamp") \
        .select("data.*", "timestamp") \
        .withColumn("timestamp_ts", from_unixtime(col("timestamp") / 1000).cast("timestamp"))
    
    # Calcular OHLC con ventanas de 1 minuto
    ohlc_df = parsed_df \
        .withWatermark("timestamp_ts", "10 seconds") \
        .groupBy(
            col("symbol"),
            window(col("timestamp_ts"), "1 minute")
        ) \
        .agg(
            first("price").alias("open"),
            max("price").alias("high"),
            min("price").alias("low"),
            last("price").alias("close"),
            sum("volume").alias("volume"),
            count("price").alias("trade_count")
        ) \
        .select(
            col("symbol"),
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("open"),
            col("high"),
            col("low"),
            col("close"),
            col("volume"),
            col("trade_count")
        )
    
    print("🟢 Procesamiento iniciado. Esperando datos...")
    
    # Escribir a InfluxDB
    query = ohlc_df.writeStream \
        .outputMode("update") \
        .foreachBatch(write_to_influxdb) \
        .trigger(processingTime="10 seconds") \
        .start()
    
    query.awaitTermination()

if __name__ == "__main__":
    run_spark_streaming()