FROM python:3.6

WORKDIR /app

RUN apt-get update -qq && \
    apt-get install -qq -y build-essential libpq-dev git-core gdal-bin libgeos-dev postgresql-client && \
    pip install -U pip

COPY requirements.txt /app/
RUN pip install -r /app/requirements.txt

COPY . /app