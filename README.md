# Trading Platform - Monitor de Mercado en Tiempo Real

## Arquitectura

Esta solución implementa una arquitectura Kappa completa para el procesamiento en tiempo real de datos financieros:

Finnhub WebSocket → Kafka → Spark Streaming → InfluxDB → Grafana


### Componentes:
- **Finnhub API**: Fuente de datos en tiempo real
- **Kafka**: Broker de mensajería para ingesta de datos
- **Spark Streaming**: Procesamiento en tiempo real para cálculos OHLC
- **InfluxDB**: Base de datos de series temporales
- **Grafana**: Visualización y alertas

## Requisitos Previos

- Docker y Docker Compose
- Token de API de Finnhub (gratuito en https://finnhub.io/)
- 8GB RAM mínimo
- Puertos disponibles: 2181, 9092, 8080, 8086, 3000

## Instalación y Ejecución

### 1. Configurar Token de Finnhub

Editar el archivo `producer/finnhub_producer.py` y reemplazar `YOUR_FINNHUB_TOKEN` con tu token real.

### 2. Levantar la Infraestructura

```bash
docker-compose up -d

