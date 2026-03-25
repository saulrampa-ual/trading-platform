#!/bin/bash

# Esperar a que InfluxDB esté listo
sleep 10

# Crear bucket si no existe
influx bucket create \
  --name market_data \
  --org trading \
  --token trading-token-123 \
  --retention 30d

# Crear retention policy
influx bucket update \
  --name market_data \
  --org trading \
  --token trading-token-123 \
  --retention 30d