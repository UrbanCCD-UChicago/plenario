import requests


URLROOT = "http://plenario-app-testy-brawndo.us-east-1.elasticbeanstalk.com"

TARGETS = [
    '/v1/api/update/weekly',
]

for target in TARGETS:
    response = requests.post(URLROOT + target)
    print response
