FROM python:3.6

RUN mkdir /app
WORKDIR /app

RUN apt-get update
RUN apt-get upgrade -y
RUN apt-get install build-essential libpq-dev git-core gdal-bin libgeos-dev -y
RUN apt-get install postgresql-client -y

RUN pip install -U pip
COPY requirements.txt /app/
RUN pip install -r /app/requirements.txt

COPY . /app

