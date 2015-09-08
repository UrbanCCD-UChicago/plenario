# Inspired by Docker Python 2.7 onbuild Dockerfile,
# available at https://github.com/docker-library/python/blob/master/2.7/onbuild/Dockerfile

# python:2.7 is official Docker image for python.
# It includes most common dev dependencies (eg libxslt-dev)
FROM python:2.7
MAINTAINER willengler@uchicago.edu

RUN apt-get -y update
RUN apt-get install -y --no-install-recommends \
  # Postgres wrapper
  python-psycopg2 \
  # For Flask bcrypt
  python-bcrypt \
  # For shapely
  libgeos-dev \
  redis-server

# Clone the plenario repo
RUN mkdir -p /usr/src/plenario
RUN git clone https://github.com/UrbanCCD-UChicago/plenario.git /usr/src/plenario
WORKDIR /usr/src/plenario

RUN pip install --no-cache-dir -r requirements.txt

COPY plenario/settings.py.docker plenario/settings.py
COPY plenario/celery_settings.py.example plenario/celery_settings.py

# Start background task queuing
RUN redis server &
RUN celery -A plenario.celery_app worker --loglevel=info &

# Start the dev server with "docker run [name] [optional args]"
ENTRYPOINT ["python", "runserver.py"]
