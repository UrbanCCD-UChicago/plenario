from plenario.utils.polygon_etl import PlenarioShapeETL

censusblocks_url = 'http://www2.census.gov/geo/tiger/TIGER2010/TABBLOCK/'

state_mapping =  [
('01','Alabama'),
('02','Alaska'),
('60','American Samoa'),
('04','Arizona'),
('05','Arkansas'),
('06','California'),
('08','Colorado'),
('69','Commonwealth of the Northern Mariana Islands'),
('09','Connecticut'),
('10','Delaware'),
('11', 'District of Columbia'),
('12','Florida'),
('13','Georgia'),
('66','Guam'),
('15','Hawaii'),
('16','Idaho'),
('17','Illinois'),
('18','Indiana'),
('19','Iowa'),
('20','Kansas'),
('21','Kentucky'),
('22','Louisiana'),
('23','Maine'),
('24','Maryland'),
('25','Massachusetts'),
('26','Michigan'),
('27','Minnesota'),
('28','Mississippi'),
('29','Missouri'),
('30','Montana'),
('31','Nebraska'),
('32','Nevada'),
('33','New Hampshire'),
('34','New Jersey'),
('35','New Mexico'),
('36','New York'),
('37','North Carolina'),
('38','North Dakota'),
('39','Ohio'),
('40','Oklahoma'),
('41','Oregon'),
('42','Pennsylvania'),
('72','Puerto Rico'),
('44','Rhode Island'),
('45','South Carolina'),
('46','South Dakota'),
('47','Tennessee'),
('48','Texas'),
('78','United States Virgin Islands'),
('49','Utah'),
('50','Vermont'),
('51','Virginia'),
('53','Washington'),
('54','West Virginia'),
('55','Wisconsin'),
('56','Wyoming')]

def uscensusblocks_add_state_byname(state_name):
    state_name.lower()
    state_mapping_dict = dict(state_mapping)
    inv_state_mapping_dict = dict((v.lower(),k) for k, v in state_mapping_dict.iteritems())
    try:
        uscensusblocks_add_state(inv_state_mapping_dict[state_name])
    except KeyError, e:
        print "State not found: '%s'" % state_name

def uscensusblocks_add_state(state_id):
    d= {'dataset_name': 'census_blocks', 'business_key': 'geoid10', 'source_url': censusblocks_url + '2010/tl_2010_%s_tabblock10.zip' % state_id}
    e = PlenarioShapeETL(d)
    e._download()
    e._load_shapefile()
    e._get_or_create_table()
    e._insert_data()
    
def uscensusblocks_add_all():
    for (sid, sname) in state_mapping:
        print "adding censusblocks for ", (sid, sname)
        uscensus_add_state(sid)

