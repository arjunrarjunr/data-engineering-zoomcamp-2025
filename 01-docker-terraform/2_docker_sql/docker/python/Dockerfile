FROM docker.io/bitnami/python:3.9.7

WORKDIR /root/notebooks

ADD requirements.txt .

RUN apt-get update && \
    apt-get install -y libpq-dev && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

RUN pip install --upgrade pip

RUN pip install -r requirements.txt

EXPOSE 1234