FROM bitnami/spark:3.5.0

USER root

# Instalar dependencias de Python
RUN apt-get update && apt-get install -y python3-pip && \
    pip3 install --upgrade pip

# Copiar requirements
COPY requirements.txt /tmp/requirements.txt
RUN pip3 install -r /tmp/requirements.txt

# Copiar scripts de Spark
COPY ohlc_processor.py /opt/spark-apps/ohlc_processor.py

USER 1001