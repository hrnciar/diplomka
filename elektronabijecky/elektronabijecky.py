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

config = toml.load('config.toml')

EXIT_REQUEST_ERROR = 1
EXIT_ROLLBACK_SUCCESS = 2
EXIT_ROLLBACK_ERROR = 3

def get_data(url, period, station, pump):
    """Scrape data from web"""
    with requests.Session() as session:
        session.post(config['post_login_url'], data=config['payload'])
        # Prevention against TooManyAttemptsError.
        # I have run into this problem just once, if server complaints
        # about too many attemps it's better to use `sleep(1)`.
        # Keeping it here just for documentation purposes, run into this
        # error only once or twice.
        try:
            request = session.get(
                url + '&owner=0&period=' +
                str(period) + '&station=' +
                str(station) + '&pump=' + str(pump)
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
    tree = BeautifulSoup(raw_data.text, "lxml")
    try:
        table = tree.select("table")[1] # Choose second table "Detail"
    except Exception:
        logging.info('Skipping - no data table')
        return 1
    list_of_rows = []
    for row in table.findAll('tr'):
        list_of_cells = []
        td_counter = 0
        for cell in row.findAll(["td"]):
            text = cell.text
            list_of_cells.append(text)
            td_counter += 1
        list_of_rows.append(list_of_cells)

    del list_of_rows[1]
    del list_of_rows[-1]

    for row in list_of_rows:
        del row[0]
        del row[1:3]
        del row[2:7]
    # At this point we have double dimensional array, [[table header], [row], [row]]
    # Removes table header
    del list_of_rows[0]

    return list_of_rows

def prepare_data(list_of_rows, station, socket):
    """
        Prepares data for publishing and neccessary data to conform Open Normal Form of datas.
        https://github.com/opendata-mvcr/otevrene-formalni-normy/issues/205
    """
    index = 0
    for row in list_of_rows:
        row[0] = row[0].split(' ')
        del row[0][2] # Removed dash " - " separator from time
        date = datetime.strptime(row[0][0], '%d.%m.%Y').strftime('%Y-%m-%d')

        # YYYY-MM-DDTHH:MM:SS 'T' connects date and time (see: https://bit.ly/2y0iDP7)
        row[0][1] = date + 'T' + row[0][1] + ':00'
        row[0][2] = date + 'T' + row[0][2] + ':00'
        del row[0][0]
        consumption = row[1].split(' ')
        iri = config['resource_iri'] + config['station_dict'][station] + '/' + config['socket_dict'][socket]
        list_of_rows[index] = [iri] + row[0] + [consumption[0]] + [consumption[1]]
        index += 1

    return list_of_rows

def month_year_iter(start_month, start_year, end_month, end_year):
    ym_start = 12*start_year + start_month - 1
    ym_end = 12*end_year + end_month - 1
    for ym in range(ym_start, ym_end+1):
        y, m = divmod(ym, 12)
        if m < 10:
            current_date = str(y) + "0" + str(m + 1)
        else:
            current_date = str(y) + str(m + 1)
        logging.info('Processing %s', current_date)
        counter = 0
        for station in config['station_dict']:
            # TODO: fix this ugliness
            # This is wrong design sockets are defined in config and shoudln't be hardcoded here.
            # I will rework it if there will be some spare time before submission of my thesis,
            # for now it works. Also there probably won't be any new sockets in config so it's
            # basically just ugly.
            if station == '319':
                sockets = ['343', '344']
            else: #station 351
                sockets = ['391']
            for socket in sockets:
                raw_table = get_data(config['request_url'], current_date, station, socket)

                # list_of_rows contains prepared unprocessed data in list,
                # where each item is one row [[row], [row], [row]...]
                list_of_rows = clean_data(raw_table)
                logging.info('Data for %s cleaned', current_date)

                counter += 1

                # if table is empty, return empty list
                if list_of_rows == 1:
                    yield 'Err - empty table', y, m+1, counter
                else:
                    # data contains final form of datas, prepared to be written into file
                    data = prepare_data(list_of_rows, station, socket)
                    logging.info('Data for %s prepared', current_date)

                    yield data, y, m+1, counter

def ckan_post_request(url, action, data, headers, filename):
    """
        Wrapper around request call to provide neccessary parameters.
    """
    try:
        r = requests.post(url + action,
                          data=data,
                          headers=headers,
                          files=[('upload', open(filename, 'rb'))])
        r.raise_for_status()
    except requests.exceptions.HTTPError as e:
        logging.error(e)
        logging.error('Request for action: %s failed. Exiting...', action)
        return EXIT_REQUEST_ERROR
    except requests.exceptions.RequestException as e:
        logging.error('Request for action: %s failed. Exiting...', action)
        return EXIT_REQUEST_ERROR

    return 0

def rollback(start_year, end_year):
    rollback_error = False
    for year in range(start_year, end_year+1):
        logging.error('Rollback of year %s in progress', year)
        filename = config['filename'] + str(year) + config['extension']

        # Remove corrupted files
        try:
            os.remove(filename)
        except:
            pass
        # Restore backed up files
        copyfile(filename + '.old', filename)

        # Remove old back ups
        os.remove(filename + ".old")

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

parser = argparse.ArgumentParser(description='Import Evmapy data to CKAN')

group = parser.add_mutually_exclusive_group(required=True)
group.add_argument('-io', '--import-old', action='store_true', help='one time import of old data')
group.add_argument('-in', '--import-new', action='store_true', help='import of datas that are not yet complety, eg. first n months of year')

parser.add_argument('-sy','--start-year', action='store', type=int, required='True', help='start year of import')
parser.add_argument('-sm','--start-month', action='store', type=int, required='True', help='start month of import')
parser.add_argument('-ey', '--end-year', action='store', type=int, required='True', help='end year of import')
parser.add_argument('-em', '--end-month', action='store', type=int, required='True',help='end month of import')

group_head = parser.add_mutually_exclusive_group(required=True)
group_head.add_argument('--head', action='store_true', help='include head of table')
group_head.add_argument('--no-head', action='store_true', help='do not include head of table')

args = parser.parse_args()

logging.basicConfig(filename='elektronabijecky.log', level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

if ((len(str(args.start_year)) != 4) or (len(str(args.end_year)) != 4)):
    logging.error('Given year does not has 4 digits. Exiting...')
    exit(1)

if ((len(str(args.start_month)) >= 2) or (len(str(args.end_month)) >= 2)):
    logging.error('Given month does not has 2 digits. Exiting...')
    exit(1)

logging.debug('Arguments parsed.')
if args.head:
    head_written = False
if args.no_head:
    head_written = True

for year in range(args.start_year, args.end_year+1):
    filename = config['filename'] + str(year) + config['extension']

    # Create backup of file being uploaded
    copyfile(filename, filename + '.old')
    logging.info('Backing up %s', filename)

    if args.import_old:
        os.remove(filename)
        logging.info('Removed %s', filename)

for data, y, m, counter in month_year_iter(args.start_month, args.start_year, args.end_month, args.end_year):
    filename = config['filename'] + str(y) + config['extension'] # backup/elektronabijecky_xxxx.csv

    if os.path.exists(filename):
        append_write = 'a' # append if already exists
    else:
        append_write = 'w' # make a new file if not
    try:
        outfile = open(filename, append_write, newline='\n', encoding='utf-8')
    except IOError:
        logging.error('Could not open file for writing. Exiting...')
        exit(1)
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
    if args.import_old:
        logging.info('Importing old datas')

        if y != args.end_year:
            months_in_year = 12
        else:
            months_in_year = args.end_month

        # Ending loop after end_month iterations and all sockets proccessed
        if m == months_in_year and counter == len(config['socket_dict']):
            head_written = False

            path = os.path.join(filename)
            extension = os.path.splitext(filename)[1][1:].upper()
            resource_name = '{extension} file'.format(extension=extension)
            logging.info('Creating "{resource_name}" resource'.format(**locals()))
            data = {
                'package_id': config['package'],
                'name': y,
                'format': extension,
                'url': 'upload',  # Needed to pass validation
            }
            headers = {'Authorization': config['apikey']}
            r = ckan_post_request(config['url_api'], 'resource_create', data, headers, filename)
            if r == EXIT_REQUEST_ERROR:
                exit(rollback(args.start_year, args.end_year))

    if args.import_new:
        try:
            # TODO: use ckan_post_request() instead
            # see: https://stackoverflow.com/a/919720
            r = requests.post('http://sc02.fi.muni.cz/api/action/package_show',
                              data={'id': config['package']},
                              headers={'Authorization': config['apikey']})
            r.raise_for_status()
        except requests.exceptions.HTTPError as e:
            logging.error(e)
            logging.error('Request for retrieving resource id failed. Exiting...')
            exit(rollback(args.start_year, args.end_year))
        except requests.exceptions.RequestException as e:
            logging.error('Request for retrieving resource id failed. Exiting...')
            exit(rollback(args.start_year, args.end_year))
        data = r.json()
        resource_id = ''
        for resource in data['result']['resources']:
            now = datetime.now()
            if resource['name'] == str(now.year):
                resource_id = resource['id']

        path = os.path.join(filename)
        extension = os.path.splitext(filename)[1][1:].upper()
        resource_name = '{extension} file'.format(extension=extension)
        if resource_id == '':
            logging.info('Creating "{resource_name}" resource'.format(**locals()))
            data = {
                'package_id': config['package'],
                'name': y,
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
                'package_id': config['package'],
                'name': y,
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
    os.remove(filename + '.old')
    logging.info('Removing old backups %s', filename)