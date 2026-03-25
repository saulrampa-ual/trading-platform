import asyncio
import websockets
import json
import logging
from kafka import KafkaProducer
from datetime import datetime
import ssl

# Configuración de logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class FinnhubProducer:
    def __init__(self, kafka_broker='localhost:9092', finnhub_token='YOUR_FINNHUB_TOKEN'):
        self.kafka_producer = KafkaProducer(
            bootstrap_servers=[kafka_broker],
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        self.finnhub_token = finnhub_token
        self.symbols = ['BINANCE:BTCUSDT', 'AAPL', 'INTC']
        self.topic = 'trades'
        
    async def connect_and_stream(self):
        """Conectar a WebSocket de Finnhub y transmitir datos a Kafka"""
        websocket_url = f"wss://ws.finnhub.io?token={self.finnhub_token}"
        
        try:
            async with websockets.connect(websocket_url, ssl=ssl.create_default_context()) as websocket:
                # Suscribirse a los símbolos
                for symbol in self.symbols:
                    subscribe_msg = json.dumps({"type": "subscribe", "symbol": symbol})
                    await websocket.send(subscribe_msg)
                    logger.info(f"Suscrito a {symbol}")
                
                # Procesar mensajes entrantes
                async for message in websocket:
                    data = json.loads(message)
                    if data['type'] == 'trade':
                        for trade in data['data']:
                            trade_record = {
                                'symbol': trade['s'],
                                'price': trade['p'],
                                'volume': trade['v'],
                                'timestamp': datetime.fromtimestamp(trade['t'] / 1000).isoformat(),
                                'unix_timestamp': trade['t']
                            }
                            # Enviar a Kafka
                            self.kafka_producer.send(self.topic, value=trade_record)
                            logger.info(f"Trade enviado: {trade_record}")
                            
        except Exception as e:
            logger.error(f"Error en WebSocket: {e}")
        finally:
            self.kafka_producer.close()
    
    def run(self):
        """Ejecutar el producer"""
        asyncio.run(self.connect_and_stream())

if __name__ == "__main__":
    # Reemplazar con tu token de Finnhub
    FINNHUB_TOKEN = "YOUR_FINNHUB_TOKEN"
    producer = FinnhubProducer(finnhub_token=FINNHUB_TOKEN)
    producer.run()