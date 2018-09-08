import requests
import os
import pandas as pd
import json
import sqlite3
import datetime


zip_codes = pd.read_csv(os.path.join(os.path.dirname(__file__), 'ca_zip_codes.csv'))


def convert_epoch_to_iso(epoch_value):
    return datetime.datetime.utcfromtimestamp(epoch_value).isoformat()


def format_nppes_url(zip, city):
    nppes_url = r'https://npiregistry.cms.hhs.gov/api/?taxonomy_description=PAIN&address_purpose=LOCATION&city={city}&state=CA&postal_code={zip}&&limit=200'
    return nppes_url.format(city=city, zip=zip)


def ca_zip_nppes(zip_index):
    json_resp = requests.get(format_nppes_url(*zip_codes.iloc[zip_index].tolist())).json()
    if json_resp['result_count'] > 0:
        return json_resp['results']


class NppesResponse(object):
    def __init__(self, sqlite_db):
        self.conn = sqlite3.connect(sqlite_db)
        self.cursor = self.conn.cursor()


    def process_nppes_result(self):
