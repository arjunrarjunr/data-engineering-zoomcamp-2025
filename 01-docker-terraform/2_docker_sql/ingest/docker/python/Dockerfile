FROM docker.io/bitnami/python:3.12.8

WORKDIR /root/scripts

ADD requirements.txt .

RUN apt-get update && \
    apt-get install -y libpq-dev && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

RUN pip install --upgrade pip

RUN pip install -r requirements.txt