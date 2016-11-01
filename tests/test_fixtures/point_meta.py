flu_url = 'https://data.cityofchicago.org/api/views/rfdj-hdmf/rows.csv?accessType=DOWNLOAD'
flu_path = 'Flu_Shot_Clinic_Locations_-_2013.csv'

flu_shot_meta = {
    'dataset_name': 'flu_shot_clinics',
    'human_name': 'Flu Shot Clinic Locations',
    'attribution': 'foo',
    'description': 'bar',
    'url': flu_url,
    'update_freq': 'yearly',
    'business_key': 'event',
    'observed_date': 'date',
    'latitude': 'latitude',
    'longitude': 'longitude',
    'location': None,
    'contributor_name': 'Carlos',
    'contributor_organization': 'StrexCorp',
    'contributor_email': 'foo@bar.edu',
    'contributed_data_types': None,
    'approved_status': 'true',
    'is_socrata_source': False,
    'column_names': {"date": "DATE", "start_time": "VARCHAR", "end_time": "VARCHAR", "facility_name": "VARCHAR",
                     "facility_type": "VARCHAR", "street_1": "VARCHAR", "city": "VARCHAR", "state": "VARCHAR",
                     "zip": "INTEGER", "phone": "VARCHAR", "latitude": "DOUBLE PRECISION",
                     "longitude": "DOUBLE PRECISION", "day": "VARCHAR", "event": "VARCHAR", "event_type": "VARCHAR",
                     "ward": "INTEGER", "location": "VARCHAR"}
}

landmarks_url = 'https://data.cityofchicago.org/api/views/tdab-kixi/rows.csv?accessType=DOWNLOAD'
landmarks_path = 'Individual_Landmarks.csv'

landmarks_meta = {
    'dataset_name': 'landmarks',
    'human_name': 'Chicago Landmark Locations',
    'attribution': 'foo',
    'description': 'bar',
    'url': landmarks_url,
    'update_freq': 'yearly',
    'business_key': 'id',
    'observed_date': 'landmark_designation_date',
    'latitude': 'latitude',
    'longitude': 'longitude',
    'location': 'location',
    'contributor_name': 'Cecil Palmer',
    'contributor_organization': 'StrexCorp',
    'contributor_email': 'foo@bar.edu',
    'contributed_data_types': None,
    'approved_status': 'true',
    'is_socrata_source': False,
    'column_names': {"foo": "bar"}
}

crime_url = 'http://data.cityofchicago.org/api/views/ijzp-q8t2/rows.csv?accessType=DOWNLOAD'
crime_path = 'crime_sample.csv'

crime_meta = {
    'dataset_name': 'crimes',
    'human_name': 'Crimes',
    'attribution': 'foo',
    'description': 'bar',
    'url': crime_url,
    'update_freq': 'yearly',
    'business_key': 'id',
    'observed_date': 'date',
    'latitude': 'latitude',
    'longitude': 'longitude',
    'location': 'location',
    'contributor_name': 'Dana Cardinal',
    'contributor_organization': 'City of Nightvale',
    'contributor_email': 'foo@bar.edu',
    'contributed_data_types': None,
    'approved_status': 'true',
    'is_socrata_source': False,
    'column_names': {"foo": "bar"}
}
