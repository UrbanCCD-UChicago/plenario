FROM ubuntu
MAINTAINER hunter@hunterowens.net 

RUN echo "deb http://archive.ubuntu.com/ubuntu trusty main universe" > /etc/apt/sources.list
RUN apt-get -y update
RUN apt-get -y upgrade
RUN apt-get -y install aptitude 


RUN aptitude -y install wget git curl build-essential make gcc 
RUN aptitude -y install python-dev python-pip
RUN aptitude -y install libpq-dev python-psycopg2 python-bcrypt
RUN git clone https://github.com/UrbanCCD-UChicago/plenario.git 
RUN pip install -r plenario/requirements.txt

RUN ls

RUN cp plenario/settings.py.docker plenario/settings.py

RUN cp plenario/celery_settings.py.example plenario/celery_settings.py

RUN aptitude -y install redis-server

RUN redis server &

RUN celery -A plenario.celery_app worker --loglevel=info &

RUN python runserver.py

