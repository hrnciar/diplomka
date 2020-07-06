#!/usr/bin/python3
"""
This script is used for automatic publication of temperature datas
of Žďár nad Sázavou city into CKAN
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

EXIT_REQUEST_ERROR = 1
EXIT_ROLLBACK_SUCCESS = 2
EXIT_ROLLBACK_ERROR = 3
EXIT_MISSING_CONFIG = 5
EXIT_ARGUMENT_ERROR = 6
EXIT_FILE_ERROR = 7

def get_data(url, year, month):
    """Scrape data from web"""
    with requests.Session() as session:
        try:
            request = session.get(
                url + '&R=' +
                str(year) + '&M=' +
                str(month)
            )
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

        return request

def clean_data(raw_data):
    """ Removes data that are not suitable for publishing """

    list_of_rows = list(filter(bool, raw_data.content.decode('utf-8').splitlines()))

    if list_of_rows == ['Invalid input']:
        logging.info('Invalid input')
        return 1
    list_of_rows = [row.split(';') for row in list_of_rows]
    for row in list_of_rows:
        row[0] = row[0].replace(' ', 'T', 1)
        del row[3]

    # At this point we have double dimensional array, [[time, temp1, temp2], ...]
    # Removes table header
    del list_of_rows[0]

    return list_of_rows

def prepare_data(list_of_rows):
    """
        Prepares data for publishing and neccessary data to conform Open Normal Form of datas.
    """

    prepared_data = []
    index = 0
    for row in list_of_rows:
        prepared_data.append([row[0], config['senzor1-name'], row[1], '°C', config['senzor1-long'], config['senzor1-lat']])
        if row[2]:
            prepared_data.append([row[0], config['senzor2-name'], row[2], '°C', config['senzor2-long'], config['senzor2-lat']])
    return prepared_data

def month_year_iter(start_month, start_year, end_month, end_year):
    ym_start = 12*start_year + start_month - 1
    ym_end = 12*end_year + end_month - 1
    for ym in range(ym_start, ym_end+1):
        y, m = divmod(ym, 12)
        logging.info('Processing %s/%s', y, m + 1)
        raw_table = get_data(config['request_url'], y, m + 1)

        # list_of_rows contains prepared unprocessed data in list,
        # where each item is one row [[row], [row], [row]...]
        list_of_rows = clean_data(raw_table)
        logging.info('Data for %s/%s cleaned', y, m + 1)

        # if table is empty, return empty list
        if list_of_rows == 1:
            yield 'Err - empty table', y, m+1
        else:
            # data contains final form of datas, prepared to be written into file
            data = prepare_data(list_of_rows)
            logging.info('Data for %s/%s prepared', y, m + 1)

            yield data, y, m+1

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

def rollback(start_year, end_year):
    rollback_error = False
    for year in range(start_year, end_year+1):
        logging.error('Rollback of year %s in progress', year)
        filename = config['filename'] + str(year) + config['extension']

        try:
            # Remove corrupted files
            os.remove(filename)

            # Restore backed up files
            copyfile(filename + '.old', filename)

            # Remove old back ups
            os.remove(filename + ".old")
        except:
            pass

        extension = os.path.splitext(filename)[1][1:].upper()
        logging.info('Creating "{resource_name}" resource'.format(**locals()))
        data = {
            'package_id': config['package'],
            'name': year,
            'format': extension,
            'url': 'upload',  # Needed to pass validation
        }
        headers = {'Authorization': config['apikey']}
        r = ckan_post_request(config['url_api'], 'resource_create', data, headers, filename)

        if r == EXIT_REQUEST_ERROR:
            rollback_error = True
            logging.critical('FATAL ERROR: Rollback for year %s failed, trying to rollback the rest', year)

    if rollback_error:
        return EXIT_ROLLBACK_ERROR

    return EXIT_ROLLBACK_SUCCESS

parser = argparse.ArgumentParser(description='Import Žďár nad Sázavou temperature datas into CKAN')

parser.add_argument('-sy','--start-year', action='store', type=int, required='True', help='start year of import')
parser.add_argument('-sm','--start-month', action='store', type=int, required='True', help='start month of import')
parser.add_argument('-ey', '--end-year', action='store', type=int, required='True', help='end year of import')
parser.add_argument('-em', '--end-month', action='store', type=int, required='True',help='end month of import')

group_head = parser.add_mutually_exclusive_group(required=True)
group_head.add_argument('--head', action='store_true', help='include head of table')
group_head.add_argument('--no-head', action='store_true', help='do not include head of table')

args = parser.parse_args()

logging.basicConfig(filename='teplota.log', level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

try:
    config = toml.load('config.toml')
except:
    logging.error("Config file is missing. Exiting...")
    exit(EXIT_MISSING_CONFIG)

if ((len(str(args.start_year)) != 4) or (len(str(args.end_year)) != 4)):
    logging.error('Given year does not has 4 digits. Exiting...')
    exit(EXIT_ARGUMENT_ERROR)

if ((len(str(args.start_month)) > 2) or (len(str(args.end_month)) > 2) and
    (len(str(args.start_month)) <= 0) or (len(str(args.end_month)) <= 0)):
    logging.error('Given month does not has 2 digits. Exiting...')
    exit(EXIT_ARGUMENT_ERROR)

if ((args.start_month <= 2005) or (args.start_month <= 2006 and args.start_month < 7)):
    logging.error('There are no data to be processed before 2006/7. Exiting...')
    exit(EXIT_ARGUMENT_ERROR)

if args.start_year > args.end_year:
    logging.error('Starting month/year has to be smaller that ending month/year. Exiting...')
    exit(EXIT_ARGUMENT_ERROR)

logging.debug('Arguments parsed.')
if args.head:
    head_written = False
if args.no_head:
    head_written = True

for year in range(args.start_year, args.end_year+1):
    filename = config['filename'] + str(year) + config['extension']

    try:
        # Create backup of file being uploaded
        copyfile(filename, filename + '.old')
        logging.info('Backing up %s.old', filename)

        if args.head:
            os.remove(filename)
            logging.info('Removed %s', filename)
    except:
        logging.info('Nothing to backup')

package_updated_id = set([])
for data, y, m in month_year_iter(args.start_month, args.start_year, args.end_month, args.end_year):
    filename = config['filename'] + str(y) + config['extension'] # backup/teplota_xxxx.csv

    if os.path.exists(filename):
        append_write = 'a' # append if already exists
        logging.debug('Appending to existing file')
    else:
        append_write = 'w' # make a new file if not
        logging.debug('Writing to new file')
    try:
        outfile = open(filename, append_write, newline='\n', encoding='utf-8')
    except IOError:
        logging.error('Could not open file for writing. Exiting...')
        exit(EXIT_FILE_ERROR)
    logging.debug('File opened')
    writer = csv.writer(outfile)

    if data != 'Err - empty table':
        if not head_written:
            writer.writerow(config['table_head'])
            head_written = True
        #print(' '.join(TABLE_HEAD))
        for row in data:
            writer.writerow(row)
            #print(' '.join(data))
        outfile.close()

    if y != args.end_year:
        months_in_year = 12
    else:
        months_in_year = args.end_month

    # Reset head_written at the end of year
    if m == months_in_year:
        head_written = False

    # Check if package exists
    try:
        data={'id': config['package'] + str(y)}
        headers={'Authorization': config['apikey']}
        r = ckan_post_request(config['url_api'], 'package_show', data, headers, None
)
        data = r.json()

    except:
        logging.info('Dataset %s does not exists', config['package'] + str(y))

    # dataset does not exists or is deleted, create one
    if r == EXIT_REQUEST_ERROR or data['result']['state'] == 'deleted':
        logging.info('Creating dataset %s', config['package'] + str(y))
        data = {
            'name': config['package'] + str(y),
            'title': config['package_name'] + str(y),
            'private': False,
            'url': 'upload',  # Needed to pass validation,
            'owner_org': config['owner_org']
        }
        headers = {'Authorization': config['apikey']}
        r = ckan_post_request(config['url_api'], 'package_create', data, headers, None)
        data = r.json()

        if r == EXIT_REQUEST_ERROR:
            logging.error('Couldn\'t create dataset %s, exiting...', config['package'] + str(y))
            exit(EXIT_REQUEST_ERROR)
    # we have id of package that will be updated
    package_id = data['result']['id']
    package_updated_id.add(package_id)

    resource_id = ''
    for resource in data['result']['resources']:
        if resource['name'] == config['package_name'] + str(y):
            resource_id = resource['id']

    path = os.path.join(filename)
    extension = os.path.splitext(filename)[1][1:].upper()
    resource_name = '{extension} file'.format(extension=extension)
    if resource_id == '':
        logging.info('Creating "{resource_name}" resource'.format(**locals()))
        data = {
            'package_id': package_id,
            'name': config['package_name'] + str(y),
            'format': extension,
            'url': 'upload',  # Needed to pass validation
        }
        headers = {'Authorization': config['apikey']}
        r = ckan_post_request(config['url_api'], 'resource_create', data, headers, filename)
        if r == EXIT_REQUEST_ERROR:
            exit(rollback(args.start_year, args.end_year))
    else:
        logging.info('Updating "{resource_name}" resource'.format(**locals()))
        data = {
            'id': resource_id,
            'package_id': package_id,
            'name': config['package_name'] + str(y),
            'format': extension,
            'url': 'upload',  # Needed to pass validation
        }
        headers = {'Authorization': config['apikey']}
        r = ckan_post_request(config['url_api'], 'resource_update', data, headers, filename)
        if r == EXIT_REQUEST_ERROR:
            exit(rollback(args.start_year, args.end_year))

    logging.info('All datas successfully imported.')

for year in range(args.start_year, args.end_year+1):
    filename = config['filename'] + str(year) + config['extension']

    # Remove old backup, keep new one
    try:
        os.remove(filename + '.old')
        logging.info('Removing old backups %s', filename)
    except:
        pass

# Write to file ids of updated packages, it will be passed to paster to update DataStore
with open('../ids.txt','a') as f:
    for id in package_updated_id:
        f.write(id + '\n')

