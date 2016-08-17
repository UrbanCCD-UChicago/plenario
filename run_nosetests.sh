#!/bin/sh

echo "Clearing database."
env PGPASSWORD=password psql -c "drop database plenario_test" -U postgres 
echo "Creating new database."
env PGPASSWORD=password psql -c "create database plenario_test" -U postgres
env PGPASSWORD=password psql -c "create extension postgis" -U postgres -d plenario_test
python init_db.py

echo "Start redis!"
redis-server &
redis-cli FLUSHALL

echo "Running nosetests."
nosetests tests/points -vv
nosetests tests/shapes -vv
nosetests tests/models -vv
nosetests tests/submission -vv
nosetests tests/jobs -vv

echo "Kill the cache and workers just in case."
pkill redis
pkill python
