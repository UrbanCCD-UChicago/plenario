# index
# -----
# restaurants_post_data (dataset)
# boundaries_post_data (shapeset)

# post data
# -----------------------------------------------------------------------------

restaurants_post_data = dict([
    ('col_name_decisiontargetdate', u''),
    ('col_name_classificationlabel', u''),
    ('col_name_publicconsultationenddate', u''),
    ('col_name_locationtext', u''),
    ('view_url', u'https://opendata.bristol.gov.uk/api/views/5niz-5v5u/rows'),
    ('dataset_description', u'Planning applications details for applications '
                            u'from 2010 to 2014. Locations have been geocoded '
                            u'based on postcode where available.'),
    ('col_name_decisionnoticedate', u''),
    ('col_name_casetext', u''),
    ('update_frequency', u'yearly'), ('col_name_status', u''),
    ('col_name_location', u'location'),
    ('col_name_publicconsultationstartdate', u''),
    ('contributor_email', u'look@me.com'),
    ('col_name_decision', u''), ('col_name_decisiontype', u''),
    ('col_name_organisationuri', u''),
    ('col_name_appealref', u''),
    ('col_name_coordinatereferencesystem', u''),
    ('col_name_appealdecision', u''),
    ('col_name_geoarealabel', u''),
    ('col_name_organisationlabel', u''),
    ('contributor_organization', u''),
    ('col_name_casereference', u''),
    ('col_name_latitude', u''),
    ('col_name_servicetypelabel', u''),
    ('is_shapefile', u'false'),
    ('col_name_groundarea', u''), ('col_name_postcode', u''),
    ('col_name_agent', u''), ('col_name_classificationuri', u''),
    ('col_name_geoy', u''),
    ('col_name_geox', u''), ('col_name_uprn', u''),
    ('col_name_geopointlicencingurl', u''),
    ('col_name_appealdecisiondate', u''),
    ('col_name_decisiondate', u''), ('col_name_extractdate', u'observed_date'),
    ('col_name_servicetypeuri', u''), ('col_name_casedate', u''),
    ('dataset_attribution', u'Bristol City Council'),
    ('col_name_caseurl', u''), ('contributor_name', u'mrmeseeks'),
    ('col_name_publisheruri', u''), ('col_name_geoareauri', u''),
    ('col_name_postcode_sector', u''),
    ('file_url', u'https://opendata.bristol.gov.uk/api/views/5niz-5v5u/'
                 'rows.csv?accessType=DOWNLOAD'),
    ('col_name_postcode_district', u''),
    ('col_name_publisherlabel', u''), ('col_name_responsesfor', u''),
    ('col_name_responsesagainst', u''), ('col_name_longitude', u''),
    ('dataset_name', u'restaurant_applications')
])

boundaries_post_data = dict([
    ('dataset_attribution', u'City of Chicago'), 
    ('contributor_name', u'mrmeseeks'), 
    ('view_url', u''),
    ('file_url', u'https://data.cityofchicago.org/api/geospatial/bbvz-uum9'
     '?method=export&format=Shapefile'),
    ('contributor_organization', u''),
    ('dataset_description', u'Neighborhood'
                            u'boundaries in Chicago, as developed by'
                            u'the Office of Tourism. These boundaries'
                            u'are approximate and names are not'
                            u'official. The data can be viewed on the'
                            u'Chicago Data Portal with a web browser.'
                            u'However, to view or use the files'
                            u'outside of a web browser, you will need'
                            u'to use compression software and special'
                            u'GIS software, such as ESRI ArcGIS'
                            u'(shapefile) or Google Earth (KML or'
                            u'KMZ), is required.'),
    ('update_frequency', u'yearly'), ('contributor_email', u'look@me.com'),
    ('is_shapefile', u'true'), ('dataset_name', u'boundaries_neighborhoods')
])
