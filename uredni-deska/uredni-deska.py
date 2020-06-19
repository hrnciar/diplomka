#!/usr/bin/python3
"""
This script is used for automatic publication of Evmapy datas
into CKAN
"""
import os
import csv
import argparse
from datetime import datetime
import logging
import requests
from bs4 import BeautifulSoup
import toml
from shutil import copyfile

import xml.etree.ElementTree as ET

config = toml.load('config.toml')

EXIT_REQUEST_ERROR = 1
EXIT_ROLLBACK_SUCCESS = 2
EXIT_ROLLBACK_ERROR = 3

def get_data(id, filename):
    """Scrape data from web"""
    data = {'fIDS': id}
    with requests.Session() as session:
        try:
            request = session.post(config['request_url'], data = data)
        except requests.exceptions.ConnectionError as e:
            logging.error(e)
            logging.error('Request for retrieving table data failed. Exiting...')
            exit(EXIT_REQUEST_ERROR)
        except requests.exceptions.HTTPError as e:
            logging.error(e)
            logging.error('Request for retrieving table data failed. Exiting...')
            exit(EXIT_REQUEST_ERROR)
        except requests.exceptions.RequestException as e:
            logging.error(e)
            logging.error('Request for retrieving table data failed. Exiting...')
            exit(EXIT_REQUEST_ERROR)

    request.encoding = request.apparent_encoding
    #soup = BeautifulSoup(raw_table.content, features='lxml')

    root = ET.fromstring(request.text)

    tree = ET.ElementTree(root)
    tree.write(filename)
    return request


def ckan_post_request(url, action, data, headers, filename):
    """
        Wrapper around request call to provide neccessary parameters.
    """
    if filename:
        files=[('upload', open(filename, 'rb'))]
    else:
        files=None
    try:
        r = requests.post(url + action,
                          data=data,
                          headers=headers,
                          files=files)
        r.raise_for_status()
    except requests.exceptions.HTTPError as e:
        logging.error(e)
        return EXIT_REQUEST_ERROR
    except requests.exceptions.RequestException as e:
        return EXIT_REQUEST_ERROR

    if action in ['package_show', 'package_create']:
        return r
    else:
        return 0

parser = argparse.ArgumentParser(description='Import datas of City Council\'s Voting to CKAN')

parser.add_argument('-sid', '--start-id', action='store', type=int, required='True', help='starting id of import')
parser.add_argument('-eid', '--end-id', action='store', type=int, required='True',help='end id of import')

args = parser.parse_args()

logging.basicConfig(filename='elektronabijecky.log', level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

if ((len(str(args.start_id)) > 4) or (len(str(args.end_id)) > 4)):
    logging.error('Given year does not has 4 digits. Exiting...')
    exit(1)

if args.start_id > args.end_id:
    logging.error('Starting id has to be smaller than ending id. Exiting...')
    exit(1)

print(args.start_id)
print(args.end_id)

logging.debug('Arguments parsed.')

for id in range(args.start_id, args.end_id + 1):
    print(id)
    now = datetime.now()
    logging.info('Processing %s', id)
    filename = config['package'] + str(now.year) + ".xml"
    get_data(id, filename)

    # dataset does not exists or is deleted, create one
    logging.info('Creating dataset %s', config['package'] + str(id))
    data = {
        'name': config['package'] + str(now.year),
        'title': config['package_name'] + str(now.year),
        'private': False,
        'url': 'upload',  # Needed to pass validation,
        'owner_org': 'elektronabijecky-zdar-nad-sazavou' #TODO: config
    }
    headers = {'Authorization': config['apikey']}
    r = ckan_post_request(config['url_api'], 'package_create', data, headers, None)
    data = r.json()

    if r == EXIT_REQUEST_ERROR:
        logging.error('Couldn\'t create dataset %s, exiting...', config['package'] + str(now.year))
        exit(1)

    # we have id of package that will be updated
    package_id = data['result']['id']

    resource_id = ''
    for resource in data['result']['resources']:
        if resource['name'] == config['package_name'] + str(now.year):
            resource_id = resource['id']

    path = os.path.join(filename)
    extension = os.path.splitext(filename)[1][1:].upper()
    resource_name = '{extension} file'.format(extension=extension)
    if resource_id == '':
        logging.info('Creating "{resource_name}" resource'.format(**locals()))
        data = {
            'package_id': package_id,
            'name': config['package_name'] + str(now.year),
            'format': extension,
            'url': 'upload',  # Needed to pass validation
        }
        headers = {'Authorization': config['apikey']}
        r = ckan_post_request(config['url_api'], 'resource_create', data, headers, filename)

logging.info('All datas successfully imported.')

