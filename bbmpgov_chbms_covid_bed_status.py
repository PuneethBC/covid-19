import os
import re
import time
import urllib.request
from bs4 import BeautifulSoup
import pandas as pd

os.environ["https_proxy"] = "https://i0001029:AWSSMODT%40123@fr0-proxydwp-vip.eu.airbus.corp:3128"
os.environ["http_proxy"] = "http://i0001029:AWSSMODT%40123@fr0-proxydwp-vip.eu.airbus.corp:3128"

url = 'https://bbmpgov.com/chbms/'

req_table_titles = [
    'Government Hospitals (Covid Beds)',
    'Government Medical Colleges (Covid Beds)',
    'Private Hospitals (Government Quota Covid Beds)',
    'Private Medical Colleges (Government Quota Covid Beds)',
    'Government Covid Care Centers (CCC)'
    ]

def find_req_table(h4s, tables, req_table_titles):
    
    if len(h4s) == 0:
        return None
    
    if len(tables) == 0:
        return None
    
    cur_title = ''
    h4_line_num = 0
    
    for h4 in h4s:
        for h4_content in h4.contents:
            if any(h4_content in title for title in req_table_titles):
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
    
    # cond1 = (table_vals[('Net Available Beds for C+ Patients', 'Gen')] > 0)
    # cond2 = (table_vals[('Net Available Beds for C+ Patients', 'HDU')] > 0)
    # cond3 = (table_vals[('Net Available Beds for C+ Patients', 'ICU')] > 0)
    # cond4 = (table_vals[('Net Available Beds for C+ Patients', 'ICUVentl')] > 0)
    # req_table_rows = table_vals.loc[cond1 | cond2 | cond3 | cond4]
    
    # req_col1 = ('Dedicated Covid Healthcare Centers (DCHCs)', 'Name of facility')
    # req_col2 = ('Net Available Beds for C+ Patients', 'Gen')
    # req_col3 = ('Net Available Beds for C+ Patients', 'HDU')
    # req_col4 = ('Net Available Beds for C+ Patients', 'ICU')
    # req_col5 = ('Net Available Beds for C+ Patients', 'ICUVentl')
    # req_table_cols = req_table_rows[[req_col1, req_col2, req_col3, req_col4, req_col5]]
    
    # print(req_table_cols)
    # print('')
    
    return [cur_title, table_vals]

def find_tables_infos(soup, search_tags):
    
    table_infos = []

    divs = soup.find_all(search_tags[0][0], {'class': search_tags[0][1]})
    for div in divs:
        
        h4s = div.find_all(search_tags[1][0])
        tables = div.find_all(search_tags[2][0])
        
        table_info = find_req_table(h4s, tables, req_table_titles)
        
        if table_info is not None:
            table_infos.append(table_info)
        
    return table_infos

def url_connect(url):
    
    # open a connection to a URL using urllib
    webUrl  = urllib.request.urlopen(url)

    #get the result code and print it
    print ("result code: " + str(webUrl.getcode()))
    
    return webUrl

def find_bed_availability_changes(ref_tables_infos, cur_tables_infos):
    
    bed_changes_infos = []
    
    for ref_table_infos in ref_tables_infos:
        ref_table_title = ref_table_infos[0]
        for cur_table_infos in cur_tables_infos:
            cur_table_title = cur_table_infos[0]
            if ref_table_title == cur_table_title:
                bed_change_infos = ref_table_infos[1].compare(cur_table_infos[1])
                if not bed_change_infos.empty:
                    bed_changes_infos.append(bed_change_infos)

webUrl = url_connect(url)

# read the data from the URL and print it
html_text = webUrl.read()

soup = BeautifulSoup(html_text, 'html.parser')

search_tags = [['div', 'col-md-12'], ['h4'], ['table']]
ref_table_infos = find_tables_infos(soup, search_tags)

while 1:
    
    time.sleep(1)
    
    webUrl = url_connect(url)

    # read the data from the URL and print it
    html_text = webUrl.read()

    soup = BeautifulSoup(html_text, 'html.parser')

    cur_tables_infos = find_tables_infos(soup, search_tags)
    
    find_bed_availability_changes(ref_table_infos, cur_tables_infos)
    
    ref_tables_infos.clear()
    ref_tables_infos = cur_tables_infos.copy()
    
