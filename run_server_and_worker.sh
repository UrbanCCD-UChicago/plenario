#!/bin/sh

echo "Tearing down the last session."
pkill python
redis-cli FLUSHALL

echo "Running redis and worker."
redis-server &
python worker.py &

echo "Clearing database."
env PGPASSWORD=password psql -c "drop database plenario_test" -U postgres 

echo "Creating new database."
env PGPASSWORD=password psql -c "create database plenario_test" -U postgres
env PGPASSWORD=password psql -c "create extension postgis" -U postgres -d plenario_test
python init_db.py

echo "Running server."
python runserver.py
