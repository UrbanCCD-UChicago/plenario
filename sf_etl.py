from wopr import create_app
from wopr.tasks import update_crime, dat_crime, raw_crime, sf_raw_crime,\
    sf_dat_crime, import_shapefile
#celery_app = make_celery(app=app)


app = create_app()

if __name__ == "__main__":
    #app.run(debug=True, port=5001)
    #update_crime(fpath='data/crime_2014-07-08T12:03:43.csv.gz')
    #print 'Updating crimes...\n'
    #raw_crime(fpath='data/crime_subset.csv.gz')
    #dat_crime(fpath='data/crime_subset.csv.gz')
    #update_crime(fpath='data/crime_subset.csv.gz')
    #print "Starting app...\n"
    #sf_raw_crime(fpath='data/sfpd_incident_all_csv.zip')
    #sf_dat_crime(fpath='data/sfpd_incident_all_csv.zip', crime_type='violent')
    #sf_dat_crime(fpath='data/sfpd_incident_all_csv.zip', crime_type='property')
    import_shapefile('./data/sf_census_blocks.zip', 'sf_census_blocks')
    #import_shapefile('./data/CI.2.d.BlockParties.zip', 'sf_block_parties')
    #app.run(debug=True, use_reloader=False, port=5001)
