from wopr import create_app
from wopr.tasks import update_crime, dat_crime, raw_crime, sf_raw_crime,\
    sf_dat_crime, import_shapefile
#celery_app = make_celery(app=app)

proj_str = '+proj=lcc +lat_1=37.06666666666667 +lat_2=38.43333333333333 \
+lat_0=36.5 +lon_0=-120.5 +x_0=2000000 +y_0=500000.0000000002 +datum=NAD83 \
+units=us-ft +no_defs'

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
    #import_shapefile('./data/sf_census_blocks.zip', 'sf_census_blocks', proj=4326)
    import_shapefile('./data/CI.2.d.BlockParties.zip', 'sf_block_parties', proj=proj_str)
    import_shapefile('./data/HEF.2.a.OpenSpace_total.zip', 'sf_open_space', proj=proj_str)
    import_shapefile('./data/HWB.2.a.FarmersMarketAccess.zip', 'sf_farmers_markets',
        proj=proj_str)
    #import_shapefile('./data/building_footprint.zip', 'sf_building_footprint', proj=102643)
    #app.run(debug=True, use_reloader=False, port=5001)
