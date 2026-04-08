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
| Zookeeper | 2181 | Coordinador de Kafka |
| Kafka | 9092 | Broker (listener externo para el producer) |
| Kafka | 19092 | Listener interno para Spark |
| Spark Master UI | 8080 | Monitor de jobs |
| Spark Worker UI | 8081 | Estado del worker |
| InfluxDB | 8086 | TSDB — almacena velas OHLC |
| Grafana | 3000 | Dashboard y alertas |

---

## Requisitos previos

- [Docker Desktop](https://www.docker.com/products/docker-desktop/) instalado y corriendo
- Python 3.9 o superior instalado en el host
- Token de API de Finnhub (gratuito en <https://finnhub.io/dashboard>)

---

## Pasos para levantar el entorno

### 1. Clona el repositorio
```bash
git clone <url-del-repo>
cd trading-platform
```

### 2. Obtén tu token de Finnhub

Regístrate gratis en <https://finnhub.io/dashboard> y copia tu API key.

### 3. Edita el producer

Abre `producer/finnhub_producer.py` y sustituye:
```python
FINNHUB_TOKEN = "TU_TOKEN_AQUI"
```

### 4. Construye y levanta la infraestructura Docker
```bash
# Primera vez: construye la imagen Spark personalizada
# Tarda ~3-5 min porque descarga los JARs de Kafka desde Maven Central
docker compose build

# Levanta todos los servicios en segundo plano
docker compose up -d

# Comprueba que todos los contenedores están corriendo
docker compose ps
```

Deberías ver 6 contenedores en estado `Up`:
```
trading-zookeeper   Up
trading-kafka       Up
trading-influxdb    Up
spark-master        Up
spark-worker        Up
trading-grafana     Up
```

### 5. Da permisos al directorio de checkpoints de Spark
```bash
docker exec -u root spark-master mkdir -p /tmp/spark-checkpoints/ohlc
docker exec -u root spark-master chmod -R 777 /tmp/spark-checkpoints
```

### 6. Instala dependencias del producer en el host
```bash
pip install confluent-kafka websockets requests
```

### 7. Ejecuta el producer
```bash
python producer/finnhub_producer.py
```

Verás logs como:
```
✅ Suscrito a BINANCE:BTCUSDT
✅ Suscrito a AAPL
✅ Suscrito a INTC
🟢 Escuchando trades — publicando en Kafka topic 'trades'…
💰 BINANCE:BTCUSDT  $67432.1000  vol=0.002300
```

Deja esta terminal corriendo y abre una nueva para el paso siguiente.

### 8. Lanza el job de Spark
```bash
docker exec spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 /opt/spark/apps/ohlc_processor.py
```

Cuando veas lo siguiente el pipeline está funcionando de extremo a extremo:
```
🟢 Procesamiento iniciado. Esperando datos de Kafka…
✅ Batch 1: 3 velas guardadas
```

> **Importante:** No lances más de un spark-submit a la vez. Si necesitas
> relanzarlo, ve primero a <http://localhost:8080> y haz clic en **(kill)**
> en el job anterior para liberar los cores del worker.

### 9. Accede a Grafana e importa el dashboard

- URL: <http://localhost:3000>
- Usuario: `admin` / Contraseña: `admin`

El datasource **InfluxDB** ya está provisionado automáticamente.

**Pasos para importar el dashboard:**

1. Menú izquierdo → **Dashboards** → **Import**
2. Sube el archivo `grafana/dashboards/trading-dashboard.json`
3. Pulsa **Import** o **Import (Overwrite)**

Deberías ver el dashboard con 3 paneles actualizándose cada 5 segundos:
- **Último Precio BTCUSDT** — stat con el precio más reciente
- **Gráfico de Velas - BTCUSDT** — evolución OHLC
- **Volumen por Minuto** — barras de volumen

> **Nota sobre el UID del datasource:** Si los paneles muestran "No data"
> tras importar, ve a **Connections → Data sources → InfluxDB** y copia
> el UID que aparece en la URL del navegador
> (`/connections/datasources/edit/XXXXXXXX`). Luego reemplaza las 3
> apariciones de `"uid": "influxdb"` en el JSON por ese UID y vuelve
> a importar.

### 10. Verifica las alertas

Ve a **Alerting → Alert rules** en Grafana. Deberías ver la regla
`Alerta de volatilidad BTCUSDT` provisionada automáticamente. Se dispara
cuando el spread (high - low) / low supera el 0.5% en los últimos 5 minutos.

---

## Flujo de datos detallado

1. **Finnhub WebSocket** emite eventos de trade con formato `{s, p, v, t}`
   donde `s` = símbolo, `p` = precio, `v` = volumen, `t` = timestamp ms.
2. El **producer** (`finnhub_producer.py`) normaliza cada evento a
   `{symbol, price, volume, timestamp}` y lo publica en el topic `trades`
   de Kafka usando `localhost:9092`.
3. **Spark Structured Streaming** (`ohlc_processor.py`) consume el topic
   `trades` desde `kafka:19092` (red interna Docker), parsea el JSON y
   aplica una *tumbling window* de 1 minuto por símbolo para calcular
   `open`, `high`, `low`, `close` y `volume` total.
4. Cada micro-batch (cada 10 s) escribe las velas completadas en
   **InfluxDB** usando el Line Protocol en el bucket `market_data`.
5. **Grafana** consulta InfluxDB mediante Flux y renderiza el gráfico de
   velas, el volumen por minuto, el último precio y las alertas de
   volatilidad. El dashboard se refresca automáticamente cada 5 segundos.

---

## Solución de problemas comunes

### Spark se cae al arrancar (`Exited (0)`)

La imagen `apache/spark:4.0.0` no usa la variable `SPARK_MODE` de Bitnami.
Los servicios deben tener un `command` explícito en `docker-compose.yml`:
```yaml
spark-master:
  command: ["/opt/spark/bin/spark-class", "org.apache.spark.deploy.master.Master"]

spark-worker:
  command: ["/opt/spark/bin/spark-class", "org.apache.spark.deploy.worker.Worker", "spark://spark-master:7077"]
```

### Error `mkdir of /tmp/spark-checkpoints failed`

El usuario `spark` no tiene permisos de escritura. Ejecuta:
```bash
docker exec -u root spark-master chmod -R 777 /tmp/spark-checkpoints
```

### Error `NoSuchMethodError: setMinEvictableIdleDuration`

Incompatibilidad entre `commons-pool2` y Spark 4.0. Asegúrate de usar
la versión `2.12.0` en el `Dockerfile.spark`, no la `2.11.1`:
```dockerfile
curl -fsSL \
  https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.12.0/commons-pool2-2.12.0.jar \
  -o /opt/spark/jars/commons-pool2-2.12.0.jar
```

### Job de Spark en estado WAITING

Hay otro job anterior ocupando los cores del worker. Ve a
<http://localhost:8080>, localiza el job en estado `RUNNING` y haz clic
en **(kill)**. El job en WAITING pasará a RUNNING automáticamente.

### Kafka no arranca

Asegúrate de que `KAFKA_LISTENERS` está definido en `docker-compose.yml`:
```yaml
KAFKA_LISTENERS: PLAINTEXT://0.0.0.0:9092,PLAINTEXT_INTERNAL://0.0.0.0:19092
```

Sin esta variable Kafka no sabe en qué interfaces escuchar y falla al
arrancar.

### Producer no conecta a Kafka

El producer corre en el **host**, por eso usa `localhost:9092`.
Spark corre **dentro de Docker**, por eso usa `kafka:19092`.
Ambos listeners están definidos en `KAFKA_ADVERTISED_LISTENERS`.

### Paneles de Grafana muestran "No data"

El UID del datasource en el JSON no coincide con el asignado por Grafana.
Ver la nota del paso 9.

### Warning "Current batch is falling behind"

Es normal en máquinas con recursos limitados. Spark tarda más de 10 s en
procesar el batch pero los datos llegan correctamente a InfluxDB. No es
un error.