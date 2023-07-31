FROM apache/airflow:2.6.2

ADD requirements.txt .

# docker build . -t airflow_more:latest

RUN pip install --user --upgrade pip
RUN pip install --no-cache-dir --user -r requirements.txt