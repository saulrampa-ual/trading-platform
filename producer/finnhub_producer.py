import json
import time
import asyncio
import websockets
import ssl
from confluent_kafka import Producer
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

KAFKA_BROKER = 'localhost:9092'
KAFKA_TOPIC = 'trades'
FINNHUB_TOKEN = 'd7210g9r01nieeeifa2a0d7210g9r01nieeeifa2a0'
SYMBOLS = ['BINANCE:BTCUSDT', 'AAPL', 'INTC']

def delivery_report(err, msg):
    if err is not None:
        logger.error(f"Error al entregar mensaje a Kafka: {err}")
    else:
        logger.debug(f"Mensaje entregado a {msg.topic()} [Partition: {msg.partition()}]")

async def connect_and_stream():
    """Conectar a WebSocket de Finnhub y publicar en Kafka"""
    
    # Configurar productor Kafka
    conf = {'bootstrap.servers': KAFKA_BROKER}
    producer = Producer(**conf)
    
    websocket_url = f"wss://ws.finnhub.io?token={FINNHUB_TOKEN}"
    
    try:
        async with websockets.connect(websocket_url, ssl=ssl.create_default_context()) as websocket:
            # Suscribirse a los símbolos
            for symbol in SYMBOLS:
                subscribe_msg = json.dumps({"type": "subscribe", "symbol": symbol})
                await websocket.send(subscribe_msg)
                logger.info(f"✅ Suscrito a {symbol}")
            
            logger.info("🟢 Escuchando trades en tiempo real...")
            
            async for message in websocket:
                data = json.loads(message)
                if data['type'] == 'trade':
                    for trade in data['data']:
                        trade_record = {
                            'symbol': trade['s'],
                            'price': trade['p'],
                            'volume': trade['v'],
                            'timestamp': trade['t']
                        }
                        # Enviar a Kafka
                        data_bytes = json.dumps(trade_record).encode('utf-8')
                        producer.produce(KAFKA_TOPIC, key=trade['s'], value=data_bytes, callback=delivery_report)
                        producer.poll(0)
                        logger.info(f"💰 Trade: {trade['s']} - ${trade['p']} - Vol: {trade['v']}")
                        
    except Exception as e:
        logger.error(f"❌ Error en WebSocket: {e}")
    finally:
        logger.info("Vaciando mensajes pendientes...")
        producer.flush()

def run_producer():
    """Ejecutar el productor"""
    logger.info(f"Conectando a Kafka en {KAFKA_BROKER}...")
    asyncio.run(connect_and_stream())

if __name__ == "__main__":
    run_producer()