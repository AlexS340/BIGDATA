FROM apache/airflow:2.8.1

USER root

RUN sudo apt update && \
    sudo apt-get install -y libpq-dev gcc

USER 50000

COPY requirements.txt /requirements.txt
RUN pip install --user --upgrade pip
RUN pip install --no-cache-dir --user -r /requirements.txt
