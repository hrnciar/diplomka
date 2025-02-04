#!/usr/bin/python3
"""
This script is used for automatic publication of Evmapy datas
into CKAN
"""
import os
import argparse
from datetime import datetime
import logging
import xml.etree.ElementTree as ET
import requests
import toml

EXIT_REQUEST_ERROR = 1
EXIT_ROLLBACK_SUCCESS = 2
EXIT_ROLLBACK_ERROR = 3
EXIT_NOTHING_TO_UPLOAD = 4
EXIT_MISSING_CONFIG = 5

def get_data(path, id):
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

    try:
        root = ET.fromstring(request.text)
    except ET.ParseError:
        logging.info('There is no dataset with given ID yet. Nothing to upload. Exiting...')
        exit(EXIT_NOTHING_TO_UPLOAD)

    #print(ET.tostring(root))
    date = datetime.strptime(root[0].text, '%d.%m.%Y').strftime('%Y-%m-%d')
    tree = ET.ElementTree(root)
    filename = config['filename'] + str(date) + ".xml"
    tree.write(path + '/'  + filename)
    return date, filename


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

dirname = os.path.realpath(__file__)
location = dirname.rsplit('/',1)[0]
filename = location + '/config.toml'

logging.basicConfig(filename=location + "/uredni-deska.log", level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

try:
    config = toml.load(filename)
except:
    logging.error("Config file is missing. Exiting...")
    exit(EXIT_MISSING_CONFIG)

if ((len(str(args.start_id)) > 4) or (len(str(args.end_id)) > 4)):
    logging.error('Given year does not has 4 digits. Exiting...')
    exit(1)

if args.start_id > args.end_id:
    logging.error('Starting id has to be smaller than ending id. Exiting...')
    exit(1)

logging.debug('Arguments parsed.')

for id in range(args.start_id, args.end_id + 1):
    logging.info('Processing %s', id)
    date, filename = get_data(location, id)

    # Check if package exists
    try:
        data={'id': config['package'] + str(date)}
        headers={'Authorization': config['apikey']}
        r = ckan_post_request(config['url_api'], 'package_show', data, headers, None)
        data = r.json()
    except:
        logging.info('Dataset %s does not exists', config['package'] + str(date))

    # dataset does not exists or is deleted, create one
    if r == EXIT_REQUEST_ERROR or data['result']['state'] == 'deleted':
        logging.info('Creating dataset %s', config['package'] + str(id))
        data = {
            'name': config['package'] + str(date),
            'title': config['package_name'] + str(date),
            'private': False,
            'url': 'upload',  # Needed to pass validation,
            'owner_org': config['owner_org']
        }
        headers = {'Authorization': config['apikey']}
        r = ckan_post_request(config['url_api'], 'package_create', data, headers, None)

        if r == EXIT_REQUEST_ERROR:
            logging.error('Couldn\'t create dataset %s, exiting...', config['package'] + str(date))
            exit(1)

        data = r.json()

    # we have id of package that will be updated
    package_id = data['result']['id']

    resource_id = ''
    for resource in data['result']['resources']:
        if resource['name'] == config['package_name'] + str(date):
            resource_id = resource['id']

    path = os.path.join(filename)
    extension = os.path.splitext(filename)[1][1:].upper()
    resource_name = '{extension} file'.format(extension=extension)
    if resource_id == '':
        logging.info('Creating "{resource_name}" resource'.format(**locals()))
        data = {
            'package_id': package_id,
            'name': config['package_name'] + str(date),
            'format': extension,
            'url': 'upload',  # Needed to pass validation
        }
        headers = {'Authorization': config['apikey']}
        r = ckan_post_request(config['url_api'], 'resource_create', data, headers, location + '/' +filename)

    else:
        logging.info('Updating "{resource_name}" resource'.format(**locals()))
        data = {
            'id': resource_id,
            'package_id': package_id,
            'name': config['package_name'] + str(date),
            'format': extension,
            'url': 'upload',  # Needed to pass validation
        }
        headers = {'Authorization': config['apikey']}
        r = ckan_post_request(config['url_api'], 'resource_update', data, headers, location + '/' + filename)

logging.info('All datas successfully imported.')

