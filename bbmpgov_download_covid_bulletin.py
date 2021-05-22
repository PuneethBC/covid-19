import os
import sys
import re
import time
import ast
import copy
import argparse
import random
import glob

from datetime import datetime
from dateutil.relativedelta import relativedelta

# For fetching data
import urllib.request

from bs4 import BeautifulSoup
import pandas as pd

# For sending sms
import requests
from requests.exceptions import RequestException


# Added to resolve SSL errors
PATH_env_var = 'PATH'
PATH_env_paths = os.environ.get(PATH_env_var)

anaconda_base_path = os.environ.get('CONDA_PREFIX')
# anaconda_base_path = 'C:/Users/punee/.conda/envs/kaggle'
print('Anaconda base path set to:', anaconda_base_path)

PATH_env_paths += ';' + anaconda_base_path
PATH_env_paths += ';' + os.path.join(anaconda_base_path, 'scripts')
PATH_env_paths += ';' + os.path.join(anaconda_base_path, 'Library', 'bin')
PATH_env_paths += ';' + os.path.join(anaconda_base_path, 'DLL')
os.environ[PATH_env_var] = PATH_env_paths


# Command line arguments
parser = argparse.ArgumentParser()

parser.add_argument('--tags', type=str, help='the hyperlink tag to look for', default='View')
parser.add_argument('--from_date', type=str, help='date to download from in YYYYMMDD format', default='today')
parser.add_argument('--save_dir', type=str, help='where to save the downloaded files', default='./')

args = parser.parse_args()


# URL to fetch from
bbmp_covid_bulletin_url = 'https://bbmp.gov.in/covid-master/covid19/bulletine.html'
base_data_link = 'https://bbmp.gov.in/covid-master/covid19/'
saved_file_basename = 'Covid_Bengaluru'

retry_wait_time_sec = 5
routine_check_time_th_sec = 60 * 60

def save_daily_statistics_files(soup, search_tags, hyperlink_tag, from_yyyymmdd, save_dir):
    
    end_file_download = False

    latest_downloaded_date = 0

    table_infos = []

    tag0_infos = soup.find_all(search_tags[0][0], {'class': search_tags[0][1]})
    for tag0_info in tag0_infos:
        
        tag1_infos = tag0_info.find_all(search_tags[1][0])

        for tag1_info in tag1_infos:

            tag1_vals = pd.read_html(str(tag1_info))[0]

            num_columns = len(tag1_vals.columns)
            tag1_cols = list(tag1_vals.columns)

            date_idx = tag1_cols.index(search_tags[2][0])

            hl_idx = -1
            for c_idx,col_name in enumerate(tag1_cols):
                if hyperlink_tag in col_name:
                    hl_idx = c_idx
                    break

            if hl_idx == -1:
                print('Unable to find the hyperlink tag')

            tag3_infos = tag1_info.find_all(search_tags[3][0])
            num_infos = len(tag3_infos)

            filename_tag = ''
            for tag3_info in tag3_infos:

                tag4_infos = tag3_info.find_all(search_tags[4][0])
                if len(tag4_infos) == 0:
                    # This is the header
                    tagh_infos = tag3_info.find_all('th')
                    filename_tag = tagh_infos[0].contents[0].replace(' ', '')
                    continue

                date_strs = tag4_infos[date_idx].contents[0].split('-')
                date_yyyymmdd_str = ''.join(date_strs[::-1])
                if date_yyyymmdd_str < from_yyyymmdd:
                    end_file_download = True
                    break

                file_id_str = tag4_infos[0].contents[0]

                hl_str = tag4_infos[hl_idx].contents[0].attrs['href']
                
                filename = '%s_%s_%s_%s.pdf' % (saved_file_basename, date_yyyymmdd_str, filename_tag, file_id_str)
                dst_filepath = os.path.join(save_dir, filename)

                data_link = os.path.join(base_data_link, hl_str)
                try:
                    r = requests.get(data_link, allow_redirects=True)
                    r.raise_for_status()

                    open(dst_filepath, 'wb').write(r.content)
                    print('Saved %s to %s' % (filename, save_dir))

                    date_yyyymmdd_int = int(date_yyyymmdd_str)
                    if date_yyyymmdd_int > latest_downloaded_date:
                        latest_downloaded_date = date_yyyymmdd_int
                except RequestException as e:
                    print('Error downloading file. Will try after sometime')
                

            if end_file_download:
                break

        if end_file_download:
            break

    return latest_downloaded_date

def find_latest_dl_date(save_dir):

    latest_date = 0

    filepaths = glob.glob(save_dir + '/*.pdf')

    for filepath in filepaths:

        filename = os.basename(filepath)
        filedate = int(filename.split('_')[2])

        if filedate > latest_date:
            latest_date = filedate

    latest_date = str(latest_date)

    year = int(latest_date[:4])
    month = int(latest_date[4:6])
    day = int(latest_date[6:])
    
    last_dl_date = datetime.datetime(year,month,day,0,0,0)

    return last_dl_date

def url_connect(url, retry_wait_time_sec=5):

    conn_status = True

    while 1:

        try:
            # open a connection to a URL using urllib
            webUrl  = urllib.request.urlopen(url)
            ret_code = webUrl.getcode()
            if ret_code == 200:
                if conn_status is False:
                    print('Connection Restored')
                conn_status = True
                break
            else:
                conn_status = False
                print('Looks like there was an error in getting the data. Retrying...')
                time.sleep(retry_wait_time_sec)
        except:
            conn_status = False
            print('Looks like there is connection issue. Retrying...')
            time.sleep(retry_wait_time_sec)
    
    return webUrl

if __name__ == "__main__":

    hyperlink_tags = list(args.tags.split(','))
    from_date = args.from_date
    save_dir = args.save_dir

    # Only one tag for now
    hyperlink_tag = hyperlink_tags[0]

    # Convert from_date tag to actual
    if from_date == 'all':
        from_yyyymmdd = '00000000'
    elif from_date == 'today':
        from_yyyymmdd = datetime.today().strftime('%Y%m%d')
    elif from_date == 'pending':
        # Check the directory for the latest downloaded date
        latest_dl_date = find_latest_dl_date(save_dir)
        # And start from the next day
        nextday = latest_dl_date + datetime.timedelta(days=1)
        from_yyyymmdd = nextday.strftime('%Y%m%d')
    else:
        from_yyyymmdd = from_date

    # Create directories if they do not exist
    if not os.path.exists(save_dir):
        os.makedirs(save_dir)

    while 1:

        # Fetch the data from the url
        webUrl = url_connect(bbmp_covid_bulletin_url, retry_wait_time_sec)

        # read the data from the URL and print it
        html_text = webUrl.read()

        # Get html formatted output
        soup = BeautifulSoup(html_text, 'html.parser')

        # Tags to look for in the html file
        search_tags = [['div', 'set'], ['table'], ['Date'], ['tr'], ['td']]

        # Download the files
        latest_dl_date = save_daily_statistics_files(soup, search_tags, hyperlink_tag, from_yyyymmdd, save_dir)

        date_today = int(datetime.today().strftime('%Y%m%d'))
        if date_today == latest_dl_date:

            # File already downloaded, wait for the next day
            time_now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            time_nextday = (datetime.today() + datetime.timedelta(days=1)).strftime('%Y-%m-%d %H:%M:%S')

            # Calculate difference:
            start = datetime.strptime(time_now,'%Y-%m-%d %H:%M:%S')
            ends = datetime.strptime(time_nextday, '%Y-%m-%d %H:%M:%S')
            diff = relativedelta(ends, start)

            rem_time_secs = (diff.days*24*60*60) + (diff.hours*60*60) + (diff.minutes*60) + diff.seconds

            # Sleeep for the remaining time
            time.sleep(rem_time_secs)
        else:
            # Wait for sometime and retry
            time.sleep(routine_check_time_th_sec)

        # In continuous run, download everyday
        if from_date == 'today':
            from_yyyymmdd = datetime.today().strftime('%Y%m%d')