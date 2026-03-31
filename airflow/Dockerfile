FROM apache/airflow:3.0.0-python3.11
ARG AIRFLOW_VERSION=3.0.0
ARG CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-3.11.txt"

USER root

# Cài đặt Java và procps (lệnh ps)
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    openjdk-17-jre-headless \
    procps && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

USER airflow

# Thiết lập JAVA_HOME cho Airflow thấy
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

ENV PATH="/home/airflow/.local/bin:${PATH}"

RUN uv pip install pyspark==3.5.5

RUN uv pip install torch torchvision --index-url https://download.pytorch.org/whl/cpu && \
    uv pip install Pillow

COPY requirements.txt /requirements.txt

# install 
RUN pip install --constraint "${CONSTRAINT_URL}" -r /requirements.txt

ENV PYTHONPATH="/opt/airflow/dags:/opt/airflow/plugins:/opt/airflow/src:${PYTHONPATH}"
