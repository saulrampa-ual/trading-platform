from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, window, first, max, min, last,
    sum as _sum, count, from_unixtime
)
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType
import requests

# ── Configuración ──────────────────────────────────────────────────────────────
KAFKA_BROKER      = "kafka:19092"          # listener interno Docker
KAFKA_TOPIC       = "trades"

INFLUXDB_URL      = "http://influxdb:8086/api/v2/write"
INFLUXDB_TOKEN    = "trading-token-123"
INFLUXDB_ORG      = "trading"
INFLUXDB_BUCKET   = "market_data"
# ───────────────────────────────────────────────────────────────────────────────


def write_to_influxdb(df, epoch_id):
    """Escribe cada micro-batch a InfluxDB en formato Line Protocol."""
    if df.count() == 0:
        return

    rows = df.collect()
    lines = []
    for row in rows:
        ts_ns = int(row["window_start"].timestamp()) * 1_000_000_000
        line = (
            f"ohlc,symbol={row['symbol']} "
            f"open={row['open']},high={row['high']},"
            f"low={row['low']},close={row['close']},"
            f"volume={row['volume']},trade_count={row['trade_count']}i "
            f"{ts_ns}"
        )
        lines.append(line)

    payload = "\n".join(lines)
    try:
        response = requests.post(
            INFLUXDB_URL,
            headers={
                "Authorization": f"Token {INFLUXDB_TOKEN}",
                "Content-Type":  "text/plain; charset=utf-8",
            },
            params={
                "org":       INFLUXDB_ORG,
                "bucket":    INFLUXDB_BUCKET,
                "precision": "ns",
            },
            data=payload.encode("utf-8"),
            timeout=10,
        )
        if response.status_code != 204:
            print(f"❌ InfluxDB error [{response.status_code}]: {response.text}")
        else:
            print(f"✅ Batch {epoch_id}: {len(rows)} velas guardadas")
    except Exception as exc:
        print(f"❌ Error de conexión a InfluxDB: {exc}")


def run_spark_streaming():
    print("🚀 Iniciando sesión de Spark…")

    spark = (
        SparkSession.builder
        .appName("TradingOHLCProcessor")
        # Los jars ya están en /opt/spark/jars gracias al Dockerfile
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.streaming.checkpointLocation", "/tmp/spark-checkpoints/ohlc")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    # Esquema JSON de los trades
    trade_schema = StructType([
        StructField("symbol",    StringType(), True),
        StructField("price",     DoubleType(), True),
        StructField("volume",    DoubleType(), True),
        StructField("timestamp", LongType(),   True),
    ])

    print(f"📡 Conectando a Kafka {KAFKA_BROKER} → topic '{KAFKA_TOPIC}'…")

    kafka_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BROKER)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )

    # Parsear JSON y convertir timestamp (ms → timestamp)
    parsed_df = (
        kafka_df
        .selectExpr("CAST(value AS STRING) AS json_string")
        .select(from_json(col("json_string"), trade_schema).alias("d"))
        .select(
            col("d.symbol"),
            col("d.price"),
            col("d.volume"),
            from_unixtime(col("d.timestamp") / 1000).cast("timestamp").alias("event_time"),
        )
        .filter(col("symbol").isNotNull() & col("price").isNotNull())
    )

    # Ventanas OHLC de 1 minuto con watermark de 10 segundos
    ohlc_df = (
        parsed_df
        .withWatermark("event_time", "10 seconds")
        .groupBy(
            col("symbol"),
            window(col("event_time"), "1 minute"),
        )
        .agg(
            first("price").alias("open"),
            max("price").alias("high"),
            min("price").alias("low"),
            last("price").alias("close"),
            _sum("volume").alias("volume"),
            count("price").alias("trade_count"),
        )
        .select(
            col("symbol"),
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("open"),
            col("high"),
            col("low"),
            col("close"),
            col("volume"),
            col("trade_count"),
        )
    )

    print("🟢 Procesamiento iniciado. Esperando datos de Kafka…")

    query = (
        ohlc_df.writeStream
        .outputMode("update")
        .foreachBatch(write_to_influxdb)
        .trigger(processingTime="10 seconds")
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    run_spark_streaming()
