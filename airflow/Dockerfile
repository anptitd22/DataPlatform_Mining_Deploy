FROM apache/airflow:3.0.0
ARG AIRFLOW_VERSION=3.0.0
ARG CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-3.12.txt"

USER airflow

ENV PATH="/home/airflow/.local/bin:${PATH}"

COPY requirements.txt /requirements.txt

# install 
RUN pip install --constraint "${CONSTRAINT_URL}" -r /requirements.txt

ENV PYTHONPATH="/opt/airflow/dags:/opt/airflow/plugins:${PYTHONPATH}"
