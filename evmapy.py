#!/usr/bin/python3
"""
This script is used for automatic publication of Evmapy datas
into CKAN
"""
import os
import csv
import argparse
from datetime import datetime
import requests
from bs4 import BeautifulSoup
import ckanapi
import toml

config = toml.load('config.toml')

def get_data(url, period, station, pump):
    """Scrape data from web"""
    with requests.Session() as session:
        session.post(config['post_login_url'], data=config['payload'])
        # Prevention against TooManyAttemptsError.
        # I have run into this problem just once, if server complaints
        # about too many attemps it's better to use `sleep(1)`.
        try:
            request = session.get(
                url + '&owner=0&period=' +
                str(period) + '&station=' +
                str(station) + '&pump=' + str(pump)
            )
        except requests.exceptions.ConnectionError:
            request.status_code = "Connection refused"

        return request

def clean_data(raw_data):
    """ Removes data that are not suitable for publishing """
    tree = BeautifulSoup(raw_data.text, "lxml")
    try:
        table = tree.select("table")[1] # Choose second table "Detail"
    except:
        print('Skipping - no data table') #TODO: Raise error
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
        print("Processing: " + current_date)
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

                counter += 1

                # if table is empty, return empty list
                if list_of_rows == 1:
                    yield 'Err - empty table', y, m+1, counter
                else:
                    # data contains final form of datas, prepared to be written into file
                    data = prepare_data(list_of_rows, station, socket)

                    yield data, y, m+1, counter

def ckan_post_request(url, action, data, headers, filename):
    """
        Wrapper around request call to provide neccessary parameters.
    """
    r = requests.post(url + action,
                      data=data,
                      headers=headers,
                      files=[('upload', open(filename, 'rb'))])
    return r

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

if args.head:
    head_written = False
if args.no_head:
    head_written = True

for data, y, m, counter in month_year_iter(args.start_month, args.start_year, args.end_month, args.end_year):
    filename = config['filename'] + str(y) + config['extension']

    if os.path.exists(filename):
        append_write = 'a' # append if already exists
    else:
        append_write = 'w' # make a new file if not
    outfile = open(filename, append_write, newline='\n', encoding='utf-8')
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
        if m == 12 and counter == len(config['socket_dict']):
            head_written = False
            ckan = ckanapi.RemoteCKAN('http://sc02.fi.muni.cz/', apikey=config['apikey'])

            path = os.path.join(filename)
            extension = os.path.splitext(filename)[1][1:].upper()
            resource_name = '{extension} file'.format(extension=extension)
            print('Creating "{resource_name}" resource'.format(**locals()))
            data = {
                'package_id': config['package'],
                'name': y,
                'format': extension,
                'url': 'upload',  # Needed to pass validation
            }
            headers = {'Authorization': config['apikey']}
            r = ckan_post_request(config['url_api'], 'resource_create', data, headers, filename)
            if r.status_code != 200:
                print('Error while creating resource: {0}'.format(r.content))

    if args.import_new:
        r = requests.post('http://sc02.fi.muni.cz/api/action/package_show',
                          data={'id': config['package']},
                          headers={'Authorization': config['apikey']})
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
            print('Creating "{resource_name}" resource'.format(**locals()))
            data = {
                'package_id': config['package'],
                'name': y,
                'format': extension,
                'url': 'upload',  # Needed to pass validation
            }
            headers = {'Authorization': config['apikey']}
            r = ckan_post_request(config['url_api'], 'resource_create', data, headers, filename)
            if r.status_code != 200:
                print('Error while creating resource: {0}'.format(r.content))
        else:
            print('Updating "{resource_name}" resource'.format(**locals()))
            data = {
                'id': resource_id,
                'package_id': config['package'],
                'name': y,
                'format': extension,
                'url': 'upload',  # Needed to pass validation
            }
            headers = {'Authorization': config['apikey']}
            r = ckan_post_request(config['url_api'], 'resource_update', data, headers, filename)

