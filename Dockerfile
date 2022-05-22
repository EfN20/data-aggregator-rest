FROM python:3.8-buster

RUN pip3 install --upgrade pip setuptools

ENV PYTHONUNBUFFERED 1

RUN mkdir /data_aggregator_rest

WORKDIR /data_aggregator_rest

COPY . .

COPY ./requirements.txt /requirements.txt

RUN pip3 install -r requirements.txt
