import unittest
from plenario.views import process_suggestion


class EvilSubmitTests(unittest.TestCase):
    def test_nonsense_url(self):
        self.assertRaises(RuntimeError, process_suggestion, 'totes Non$ense')

    def test_hopeless_url(self):
        self.assertRaises(RuntimeError, process_suggestion,
                          'https://www.google.com/')


class SubmitCSVTests(unittest.TestCase):
    def test_socrata_url(self):
        sub = process_suggestion('https://data.cityofchicago.org/'
                                 'Health-Human-Services/'
                                 'Flu-Shot-Clinic-Locations-2013/g5vx-5vqf')
        self.assertEqual(sub.file_url,
                         'https://data.cityofchicago.org/api/views/'
                         'g5vx-5vqf/rows.csv?accessType=DOWNLOAD')

        expected_names = {'Date', 'Start Time', 'End Time', 'Day', 'Event',
                          'Event Type', 'Address', 'City', 'State', 'Zip',
                          'Phone', 'Community Area Number',
                          'Community Area Name', 'Ward',
                          'Latitude', 'Longitude', 'Location'}
        observed_names = {c.name for c in sub.columns}
        self.assertEqual(expected_names, observed_names)

        expected_attribution = 'City of Chicago'
        expected_description = 'List of Chicago Department of Public Health free flu clinics offered throughout the city. For more information about the flu, go to http://bit.ly/9uNhqG.'
        expected_human_name = 'Flu Shot Clinic Locations - 2013'
        self.assertEqual(sub.description_meta.description, expected_description)
        self.assertEqual(sub.description_meta.attribution, expected_attribution)
        self.assertEqual(sub.description_meta.human_name, expected_human_name)

    def test_non_socrata_url(self):
        url = 'http://plenario.s3.amazonaws.com/chicago_redlight_tickets.csv'
        sub = process_suggestion(url)
        self.assertEqual(sub.file_url, url)
        col_names = {col.name for col in sub.columns}
        expected_names = {'citation_number', 'issue_time', 'plate_number',
                          'vehicle_type', 'state', 'camera_location',
                          'latitude', 'longitude'}
        self.assertEqual(col_names, expected_names)


class SubmitShapeTests(unittest.TestCase):
    def test_socrata_url_map(self):
        url = 'https://data.cityofchicago.org/Facilities-Geographic-Boundaries/Boundaries-City/ewy2-6yfk'
        sub = process_suggestion(url, is_shapefile=True)
        expected_file_url = 'https://data.cityofchicago.org/api/geospatial/ewy2-6yfk?method=export&format=Shapefile'
        self.assertEqual(sub.file_url, expected_file_url)

        expected_description = 'City boundary of Chicago. The data can be viewed on the Chicago Data Portal with a web browser. However, to view or use the files outside of a web browser, you will need to use compression software and special GIS software, such as ESRI ArcGIS (shapefile) or Google Earth (KML or KMZ).'
        expected_attribution = 'City of Chicago'
        expected_human_name = 'Boundaries - City'
        self.assertEqual(sub.description_meta.description, expected_description)
        self.assertEqual(sub.description_meta.attribution, expected_attribution)
        self.assertEqual(sub.description_meta.human_name, expected_human_name)

    def test_socrata_url_blob(self):
        url = 'https://data.cityofchicago.org/Transportation/Major-Streets/ueqs-5wr6'
        sub = process_suggestion(url, is_shapefile=True)
        expected_file_url = 'https://data.cityofchicago.org/download/ueqs-5wr6/application/zip'
        self.assertEqual(sub.file_url, expected_file_url)
        self.assertEqual(sub.view_url, url)

    def test_non_socrata_url(self):
        url = 'http://www.statsilk.com/files/country/StatPlanet_Romania.zip'
        sub = process_suggestion(url, is_shapefile=True)
        self.assertEqual(sub.file_url, url)
