from wopr import create_app, make_celery
from wopr.tasks import update_crime
app = create_app()
celery_app = make_celery(app=app)

print 'Updating crimes...\n'
update_crime(fpath='data/crime_2014-07-08T12:03:43.csv.gz')


if __name__ == "__main__":
    app.run(debug=True, port=5001)
