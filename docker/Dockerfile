# Imagen de Airflow con Java (Spark) y el paquete midas instalado.
FROM apache/airflow:3.3.2-python3.12

USER root
RUN apt-get update \
 && apt-get install -y --no-install-recommends openjdk-17-jre-headless \
 && rm -rf /var/lib/apt/lists/* \
 && ln -s /usr/lib/jvm/java-17-openjdk-* /usr/lib/jvm/java-17  # misma ruta en amd64 y arm64 (Apple Silicon)
ENV JAVA_HOME=/usr/lib/jvm/java-17

USER airflow
WORKDIR /opt/midas
COPY --chown=airflow:root pyproject.toml README.md ./
COPY --chown=airflow:root src ./src
COPY --chown=airflow:root dbt ./dbt
RUN pip install --no-cache-dir -e .

ENV MIDAS_LAKE=/opt/midas/data/lake \
    MIDAS_LANDING=/opt/midas/data/landing \
    MIDAS_META_DB=/opt/midas/data/meta/midas.db \
    MIDAS_DUCKDB=/opt/midas/data/warehouse/midas.duckdb
