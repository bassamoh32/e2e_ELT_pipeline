FROM apache/airflow:2.9.1-python3.10

ENV AIRFLOW_HOME=/opt/airflow

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY ./dags/ ${AIRFLOW_HOME}/dags/
COPY ./plugins/ ${AIRFLOW_HOME}/plugins/

USER root
RUN chown -R airflow: ${AIRFLOW_HOME}
USER airflow