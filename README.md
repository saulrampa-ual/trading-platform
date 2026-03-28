# Trading Platform — Monitor de Mercado en Tiempo Real

## Arquitectura (Kappa)

```
Finnhub WebSocket
      │  (wss)
      ▼
finnhub_producer.py   ←  corre en el HOST
      │  localhost:9092
      ▼
   Kafka (docker)
      │  kafka:19092  (red interna)
      ▼
Spark Structured Streaming (docker)
      │  micro-batches cada 10 s
      ▼
   InfluxDB 2.7 (docker)
      │  Flux queries
      ▼
   Grafana 10.2 (docker)
```

### Componentes
| Servicio | Puerto host | Descripción |
|---|---|---|
| Kafka | 9092 | Broker (listener externo para el producer) |
| Kafka | 19092 | Listener interno para Spark |
| Spark Master UI | 8080 | Monitor de jobs |
| Spark Worker UI | 8081 | Estado del worker |
| InfluxDB | 8086 | TSDB — almacena velas OHLC |
| Grafana | 3000 | Dashboard y alertas |

---

## Pasos para levantar el entorno

### 1. Obtén tu token de Finnhub
Regístrate gratis en <https://finnhub.io/dashboard> y copia tu API key.

### 2. Edita el producer
Abre `producer/finnhub_producer.py` y sustituye:
```python
FINNHUB_TOKEN = "TU_TOKEN_AQUI"
```

### 3. Levanta la infraestructura Docker
```bash
# Primera vez: construye la imagen Spark personalizada
docker compose build

# Levanta todos los servicios
docker compose up -d

# Comprueba que todo está OK
docker compose ps
```

> **Nota:** La primera vez `docker compose build` tarda ~3-5 min porque
> descarga los JARs de Kafka ↔ Spark desde Maven Central.

### 4. Instala dependencias del producer (en el host)
```bash
pip install confluent-kafka websockets requests
```

### 5. Ejecuta el producer
```bash
python producer/finnhub_producer.py
```
Verás logs como:
```
✅ Suscrito a BINANCE:BTCUSDT
🟢 Escuchando trades…
💰 BINANCE:BTCUSDT  $67432.1000  vol=0.002300
```

### 6. Lanza el job de Spark
```bash
docker exec spark-master \
  /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /opt/spark/apps/ohlc_processor.py
```

### 7. Accede a Grafana
- URL: <http://localhost:3000>
- Usuario: `admin` / Contraseña: `admin`

El datasource **InfluxDB** ya está provisionado automáticamente.
Importa el archivo `grafana/dashboards/trading-dashboard.json` para el
dashboard de velas.

---

## Solución de problemas comunes

### `bitnami/spark:3.5.0: not found`
Imagen retirada de Docker Hub. **Solucionado** usando `apache/spark:4.0.0`
con un `Dockerfile.spark` propio que pre-instala los JARs de Kafka.

### Producer no conecta a Kafka
El producer corre en el **host**, por eso usa `localhost:9092`.  
Spark corre **dentro de Docker**, por eso usa `kafka:19092`.  
Ambos listeners están definidos en `KAFKA_ADVERTISED_LISTENERS`.

### `pyspark` no se instala en el Dockerfile
La imagen `apache/spark` ya incluye PySpark. Instalarlo de nuevo con pip
genera conflictos de versión. Se ha eliminado de `requirements.txt`.

### Spark no encuentra los JARs de Kafka
Los JARs se descargan durante el `docker compose build` y quedan en
`/opt/spark/jars`. No hace falta `--packages` en `spark-submit`.

---

## Flujo de datos detallado

1. **Finnhub WebSocket** emite eventos de trade: `{s, p, v, t}`.
2. El **producer** normaliza el mensaje a `{symbol, price, volume, timestamp}`
   y lo publica en el topic `trades` de Kafka.
3. **Spark Streaming** consume el topic, parsea el JSON y aplica una
   *tumbling window* de 1 minuto por símbolo para calcular OHLC + volumen.
4. Cada micro-batch (10 s) escribe las velas completadas en **InfluxDB**
   usando el Line Protocol.
5. **Grafana** consulta InfluxDB con Flux y renderiza el gráfico de velas,
   el volumen por minuto y las alertas de volatilidad.
