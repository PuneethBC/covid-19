import os
import sys
import re
import time
import ast
import argparse
import datetime
import logging

import requests
import urllib.request
from bs4 import BeautifulSoup
import pandas as pd
from tabulate import tabulate



parser = argparse.ArgumentParser()

parser.add_argument('--bed_types', type=str, help='bed types to search for availability', default='ICUVentl')
parser.add_argument('--wait_time_sec', type=int, help='time to wait before the next query', default=60)

args = parser.parse_args()



url = 'http://bbmpgov.com/chbms/'

hospital_categories = [
    # 'Government Quota Covid-19 Beds',
    # 'Private Arrangements By COVID-19 Patients',
    'Government Hospitals (Covid Beds)',
    'Government Medical Colleges (Covid Beds)',
    'Private Hospitals (Government Quota Covid Beds)',
    'Private Medical Colleges (Government Quota Covid Beds)',
    # 'Government Covid Care Centers (CCC)'
    ]

bed_col_title = 'Net Available Beds for C+ Patients'
hospital_col_pairs = ('Dedicated Covid Healthcare Centers (DCHCs)', 'Name of facility')

# logging.basicConfig(filename='bbmpgov_chbms_covid_bed_status.log', format='%(asctime)s - %(message)s', datefmt='%d-%b-%y %H:%M:%S', level=logging.INFO)
logging.basicConfig(filename='bbmpgov_chbms_covid_bed_status.log', format='%(message)s', level=logging.INFO)



def find_req_table(h4s, tables, hospital_categories, bed_types):
    
    if len(h4s) == 0:
        return None
    
    if len(tables) == 0:
        return None
    
    cur_title = ''
    h4_line_num = 0
    
    for h4 in h4s:
        for h4_content in h4.contents:
            if any(h4_content in title for title in hospital_categories):
                h4_line_num = h4.sourceline
                cur_title = h4_content
                break
        if h4_line_num > 0:
            break
    
    if h4_line_num == 0:
        return None
    
    min_line_dif = tables[0].sourceline - h4_line_num
    req_table = tables[0]
    for table in tables[1:]:
        line_dif = table.sourceline - h4_line_num
        if line_dif < min_line_dif:
            min_line_dif = line_dif
            req_table = table
            
    table_vals = pd.read_html(str(req_table))[0]
    
    cond = (table_vals[(bed_col_title, bed_types[0])] > 0)
    for bed_type in bed_types[1:]:
        cond = cond | (table_vals[(bed_col_title, bed_type)] > 0)
    req_table_rows = table_vals.loc[cond]

    req_cols = [hospital_col_pairs]
    for bed_type in bed_types:
        req_cols.append((bed_col_title, bed_type))
    req_table_cols = req_table_rows[req_cols]
    
    return [cur_title, req_table_cols]

def find_tables_infos(soup, search_tags, bed_types):
    
    table_infos = []

    divs = soup.find_all(search_tags[0][0], {'class': search_tags[0][1]})
    for div in divs:
        
        h4s = div.find_all(search_tags[1][0])
        tables = div.find_all(search_tags[2][0])
        
        table_info = find_req_table(h4s, tables, hospital_categories, bed_types)
        
        if table_info is not None:
            table_infos.append(table_info)
        
    return table_infos

def url_connect(url):
    
    # ctx = ssl.create_default_context()
    # ctx.check_hostname = False
    # ctx.verify_mode = ssl.CERT_NONE

    # url_data = requests.get(url)
    # htmltext = url.text

    # open a connection to a URL using urllib
    webUrl  = urllib.request.urlopen(url)

    #get the result code and print it
    # print ("result code: " + str(webUrl.getcode()))
    
    return webUrl

def find_bed_availability_changes(ref_tables_infos, cur_tables_infos, bed_types):
    
    avail_hosp_categories = []
    hosp_beds_infos = []
    
    for ref_table_infos in ref_tables_infos:
        ref_table_title = ref_table_infos[0]

        for cur_table_infos in cur_tables_infos:
            cur_table_title = cur_table_infos[0]

            if ref_table_title == cur_table_title:

                hosp_beds_info = []

                bed_change_infos = ref_table_infos[1].compare(cur_table_infos[1])
                if bed_change_infos.empty:
                    continue

                for index, row in bed_change_infos.iterrows():

                    valid_row_vals = row.dropna()
                    if valid_row_vals.empty:
                        continue

                    num_valid_rows = len(valid_row_vals)
                    for vr_idx in range(0,num_valid_rows,2):

                        iname1 = valid_row_vals.index[vr_idx][0]
                        iname2 = valid_row_vals.index[vr_idx][1]
                        bed_dif = row[(iname1,iname2,'self')] - row[(iname1,iname2,'other')]
                        if bed_dif != 0:
                            hospital_name = ref_table_infos[1].iat[index,0]
                            hosp_beds_info.append([hospital_name, bed_dif])

                if len(hosp_beds_info):
                    hosp_beds_infos.append(hosp_beds_info)
                    avail_hosp_categories.append(ref_table_title)

    return avail_hosp_categories, hosp_beds_infos


def output_cur_availability(cur_tables_infos, bed_types):

    heading = 'Current Availability:'
    logging.info('{:s}\n{:s}'.format(heading, len(heading) * '-'))

    table_header = ['Hospital Name'] + bed_types

    for cur_tables_info in cur_tables_infos:
        # Convert dataframe to list
        hosp_bed_infos = cur_tables_info[1].values
        table_header[0] = cur_tables_info[0]
        logging.info('')
        logging.info(tabulate(hosp_bed_infos, headers=table_header, numalign="center", stralign="center"))
    
    logging.info('')

def output_change_status(hosp_categories, bed_availabiliy):

    # Print info about which in which hospital beds were freed up or occupied
    heading = 'Recent Changes in Hospital Beds:'
    logging.info('{:s}\n{:s}'.format(heading, len(heading) * '-'))

    for hosp_category, bed_avail in zip(hosp_categories,bed_availabiliy):
        logging.info('')
        logging.info(tabulate(bed_avail, headers=[hosp_category, "Change"], numalign="center", stralign="center"))
    
    logging.info('')

def output_date_time():
    now = datetime.datetime.now()
    logging.info(now.strftime('%Y-%m-%d %H:%M:%S'))
    logging.info('')

def output_cur_availability_infos(cur_tables_infos, hosp_categories, bed_availabiliy, bed_types):

    if len(hosp_categories) == 0:
        return

    logging.info('\n')
    output_date_time()
    output_cur_availability(cur_tables_infos, bed_types)
    output_change_status(hosp_categories, bed_availabiliy)

def output_ref_availability_infos(cur_tables_infos, bed_types):

    logging.info('\n\n')
    output_date_time()
    output_cur_availability(cur_tables_infos, bed_types)
    return

if __name__ == "__main__":

    bed_types = list(args.bed_types.split(','))
    wait_time_sec = args.wait_time_sec

    webUrl = url_connect(url)

    # read the data from the URL and print it
    html_text = webUrl.read()

    soup = BeautifulSoup(html_text, 'html.parser')

    search_tags = [['div', 'col-md-12'], ['h4'], ['table']]
    ref_tables_infos = find_tables_infos(soup, search_tags, bed_types)

    # Log the results
    output_ref_availability_infos(ref_tables_infos, bed_types)

    while 1:
        
        if wait_time_sec > 0:
            time.sleep(wait_time_sec)
        
        webUrl = url_connect(url)

        # read the data from the URL and print it
        html_text = webUrl.read()

        # Parse the html
        soup = BeautifulSoup(html_text, 'html.parser')

        # Find current hospital bed availability
        cur_tables_infos = find_tables_infos(soup, search_tags, bed_types)
        
        # Find any changes from the previous info
        hosp_categories, bed_availabiliy = find_bed_availability_changes(ref_tables_infos, cur_tables_infos, bed_types)

        # Log the results
        output_cur_availability_infos(cur_tables_infos, hosp_categories, bed_availabiliy, bed_types)
        
        ref_tables_infos.clear()
        ref_tables_infos = cur_tables_infos.copy()
    
