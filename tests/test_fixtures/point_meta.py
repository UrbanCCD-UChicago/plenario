from hashlib import md5

flu_url = 'https://data.cityofchicago.org/api/views/rfdj-hdmf/rows.csv?accessType=DOWNLOAD'
flu_url_hash = md5(flu_url).hexdigest()

flu_shot_meta = {
    'dataset_name': u'flu_shot_clinics',
    'human_name': u'Flu Shot Clinic Locations',
    'attribution': u'foo',
    'description': u'bar',
    'source_url': flu_url,
    'source_url_hash': flu_url_hash,
    'update_freq': 'yearly',
    'business_key': u'facility name',
    'observed_date': u'date',
    'latitude': u'latitude',
    'longitude': u'longitude',
    'location': None,
    'contributor_name': u'Carlos',
    'contributor_organization': u'StrexCorp',
    'contributor_email': u'foo@bar.edu',
    'contributed_data_types': None,
    'approved_status': 'true',
    'is_socrata_source': False
}

landmarks_url = 'https://data.cityofchicago.org/api/views/tdab-kixi/rows.csv?accessType=DOWNLOAD'
landmarks_url_hash = md5(landmarks_url).hexdigest()

landmarks_meta = {
    'dataset_name': u'landmarks',
    'human_name': u'Chicago Landmark Locations',
    'attribution': u'foo',
    'description': u'bar',
    'source_url': landmarks_url,
    'source_url_hash': landmarks_url_hash,
    'update_freq': 'yearly',
    'business_key': u'landmark_name',
    'observed_date': u'landmark_designation_date',
    'latitude': u'latitude',
    'longitude': u'longitude',
    'location': u'location',
    'contributor_name': u'Cecil Palmer',
    'contributor_organization': u'StrexCorp',
    'contributor_email': u'foo@bar.edu',
    'contributed_data_types': None,
    'approved_status': 'true',
    'is_socrata_source': False
}