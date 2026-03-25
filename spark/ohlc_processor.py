from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import os

def create_spark_session():
    """Crear sesión de Spark con configuraciones optimizadas"""
    return SparkSession.builder \
        .appName("TradingOHLCProcessor") \
        .config("spark.sql.shuffle.partitions", "2") \
        .config("spark.streaming.stopGracefullyOnShutdown", "true") \
        .config("spark.sql.streaming.schemaInference", "true") \
        .getOrCreate()

def process_ohlc():
    """Procesar streaming de trades y calcular OHLC"""
    
    spark = create_spark_session()
    
    # Definir esquema para los datos de trades
    trade_schema = StructType([
        StructField("symbol", StringType(), True),
        StructField("price", DoubleType(), True),
        StructField("volume", DoubleType(), True),
        StructField("timestamp", StringType(), True),
        StructField("unix_timestamp", LongType(), True)
    ])
    
    # Leer stream de Kafka
    trades_stream = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:9092") \
        .option("subscribe", "trades") \
        .option("startingOffsets", "latest") \
        .load() \
        .select(from_json(col("value").cast("string"), trade_schema).alias("data")) \
        .select("data.*") \
        .withWatermark("unix_timestamp", "10 seconds")
    
    # Calcular OHLC con ventanas de 1 minuto
    ohlc_stream = trades_stream \
        .groupBy(
            col("symbol"),
            window(col("unix_timestamp").cast("timestamp"), "1 minute")
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
    
    # Configurar escritura a InfluxDB
    def write_to_influxdb(batch_df, batch_id):
        """Escribir cada batch a InfluxDB"""
        if batch_df.count() > 0:
            # Convertir a formato de línea de InfluxDB
            rows = batch_df.collect()
            for row in rows:
                # Crear punto de datos para InfluxDB
                influx_line = f"ohlc,symbol={row['symbol']} " \
                              f"open={row['open']},high={row['high']}," \
                              f"low={row['low']},close={row['close']}," \
                              f"volume={row['volume']},trade_count={row['trade_count']} " \
                              f"{int(row['window_start'].timestamp()) * 1000000000}"
                
                # Enviar a InfluxDB (usando API HTTP)
                import requests
                influx_url = "http://influxdb:8086/api/v2/write"
                influx_token = "trading-token-123"
                influx_org = "trading"
                influx_bucket = "market_data"
                
                response = requests.post(
                    influx_url,
                    headers={
                        "Authorization": f"Token {influx_token}",
                        "Content-Type": "text/plain"
                    },
                    params={
                        "org": influx_org,
                        "bucket": influx_bucket,
                        "precision": "ns"
                    },
                    data=influx_line
                )
                
                if response.status_code != 204:
                    print(f"Error writing to InfluxDB: {response.text}")
    
    # Escribir el stream
    query = ohlc_stream \
        .writeStream \
        .foreachBatch(write_to_influxdb) \
        .outputMode("append") \
        .trigger(processingTime="10 seconds") \
        .start()
    
    query.awaitTermination()

if __name__ == "__main__":
    process_ohlc()