import requests
import os
import pandas as pd
import json
import sqlite3
import datetime
import time
import random

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

    def process_raw_response(self, results):
        conn = self.conn
        for result in results:
            try:
                pd.DataFrame.from_dict(process_nppes_result(result), orient='index').transpose().to_sql(
                    'providers', conn, if_exists='append', index=False)
            except Exception as e:
                pd.DataFrame([[str(e), result['number']]], columns=['error_string', 'provider']).to_sql(
                    'errors', conn, index=False, if_exists='append')


def process_nppes_result(result):
    result_dict = {}
    response_keys = {
        'addresses': dict(fields=['address_1', 'address_purpose', 'city', 'postal_code', 'state', 'telephone_number'],
                          values=dict()),
        'basic': dict(fields=['authorized_official_credential', 'authorized_official_first_name',
                              'authorized_official_last_name', 'enumeration_date', 'organization_name'],
                      values=dict()),
        'taxonomies': dict(fields=['code', 'desc', 'license'], values=dict()),
        'number': dict(values=dict())
    }
    for item in result['addresses']:
        if item['address_purpose'] == 'LOCATION':
            for field in response_keys['addresses']['fields']:
                response_keys['addresses']['values'][field] = item.get(field, '')
                result_dict[field] = item[field]
            break

    for field in response_keys['basic']['fields']:
        response_keys['basic']['values'][field] = result['basic'].get(field, '')
        result_dict[field] = result['basic'].get(field, '')

    for item in result['taxonomies']:
        if item['primary']:
            for field in response_keys['taxonomies']['fields']:
                response_keys['taxonomies']['values'][field] = item.get(field, '')
                result_dict[field] = item.get(field, '')
    response_keys['number']['values']['number'] = result.get('number', '')
    result_dict['number'] = result.get('number', '')
    return result_dict


if __name__ == '__main__':
    nppes = NppesResponse('nppes.sqlite')
    for i in range(20):
        response = None
        response = ca_zip_nppes(i)
        if response is not None:
            nppes.process_raw_response(response)
        time.sleep(random.randrange(10, 20, 1)/10.0)






