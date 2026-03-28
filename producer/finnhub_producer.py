"""
finnhub_producer.py
───────────────────
Conecta al WebSocket de Finnhub y publica trades en bruto en Kafka.
Se ejecuta en el HOST (fuera de Docker), por eso usa localhost:9092.
"""

import json
import asyncio
import ssl
import logging

import websockets
from confluent_kafka import Producer

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

# ── Configuración ──────────────────────────────────────────────────────────────
KAFKA_BROKER    = "localhost:9092"   # listener externo (host → contenedor)
KAFKA_TOPIC     = "trades"

# ⚠️  Sustituye por tu token real de https://finnhub.io/dashboard
FINNHUB_TOKEN   = "d721og9r01qjeeefia20d721og9r01qjeeefia2g"

SYMBOLS = ["BINANCE:BTCUSDT", "AAPL", "INTC"]
# ───────────────────────────────────────────────────────────────────────────────


def delivery_report(err, msg):
    if err:
        logger.error("❌ Kafka delivery error: %s", err)
    else:
        logger.debug("✔ %s → %s [p%d]", msg.key(), msg.topic(), msg.partition())


async def stream_trades(producer: Producer) -> None:
    url = f"wss://ws.finnhub.io?token={FINNHUB_TOKEN}"
    ssl_ctx = ssl.create_default_context()

    logger.info("🔌 Conectando a Finnhub WebSocket…")
    async with websockets.connect(url, ssl=ssl_ctx) as ws:
        # Suscribirse a los símbolos
        for symbol in SYMBOLS:
            await ws.send(json.dumps({"type": "subscribe", "symbol": symbol}))
            logger.info("✅ Suscrito a %s", symbol)

        logger.info("🟢 Escuchando trades — publicando en Kafka topic '%s'…", KAFKA_TOPIC)

        async for raw in ws:
            msg = json.loads(raw)

            if msg.get("type") != "trade":
                continue

            for trade in msg.get("data", []):
                record = {
                    "symbol":    trade["s"],
                    "price":     trade["p"],
                    "volume":    trade["v"],
                    "timestamp": trade["t"],   # ms epoch
                }
                payload = json.dumps(record).encode()
                producer.produce(
                    KAFKA_TOPIC,
                    key=trade["s"].encode(),
                    value=payload,
                    callback=delivery_report,
                )
                producer.poll(0)   # disparar callbacks sin bloquear
                logger.info("💰 %s  $%.4f  vol=%.6f", trade["s"], trade["p"], trade["v"])


def main() -> None:
    producer = Producer({"bootstrap.servers": KAFKA_BROKER})
    logger.info("Productor Kafka creado → %s", KAFKA_BROKER)

    try:
        asyncio.run(stream_trades(producer))
    except KeyboardInterrupt:
        logger.info("🛑 Interrumpido por el usuario")
    finally:
        logger.info("Flushing mensajes pendientes…")
        producer.flush()
        logger.info("Productor cerrado.")


if __name__ == "__main__":
    main()
