# Plenar.io

RESTful API for geospatial and time aggregation across multiple Chicago open datasets. The API formerly known as WOPR.

## Running locally

``` bash
git clone git@github.com:datamade/plenario.git
cd plenario
pip install -r requirements.txt
python runserver.py
```

navigate to http://localhost:5000/

# Data

New datasets are actively being added to the plenario API. We keep track of them in this [Google Doc](https://docs.google.com/spreadsheet/ccc?key=0Au-2OHnpwhGTdGJzUWJ2SERwVXZLeDU4Y3laWFJvNEE&usp=sharing#gid=0)

# Dependencies
We used the following open source tools:

* [Flask](http://flask.pocoo.org/) - a microframework for Python web applications
* [SQL Alchemy](http://www.sqlalchemy.org/) - Python SQL toolkit and Object Relational Mapper
* [Green Unicorn](http://gunicorn.org/) - Python WSGI HTTP Server for UNIX
* [psycopg2](http://initd.org/psycopg/) - PostgreSQL adapter for the Python 
* [GeoAlchemy 2](http://geoalchemy-2.readthedocs.org/en/0.2.4/) - provides extensions to SQLAlchemy for working with spatial databases

## Team

* Derek Eder
* Eric van Zanten
* Forest Gregg

## Errors / Bugs

If something is not behaving intuitively, it is a bug, and should be reported.
Report it here: https://github.com/datamade/plenario/issues

## Note on Patches/Pull Requests
 
* Fork the project.
* Make your feature addition or bug fix.
* Commit, do not mess with rakefile, version, or history.
* Send me a pull request. Bonus points for topic branches.

## Copyright

Copyright (c) 2014 DataMade and the University of Chicago. Released under the [MIT License](https://github.com/datamade/plenario/blob/master/LICENSE).
