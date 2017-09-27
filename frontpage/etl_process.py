#!/usr/bin/env python

import click
import logging
import csv
import os
from datetime import datetime
from bs4 import BeautifulSoup as bs
import re
import urlparse
import urllib
import urllib2
import psycopg2
import json
from random import random
from time import sleep


# enable logging
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s %(levelname)s %(module)s - %(funcName)s: %(message)s',
                    datefmt="%Y-%m-%d %H:%M:%S")
logger = logging.getLogger()
logger.setLevel(logging.INFO)


##### HELPER FUNCTIONS #####

def get_post_date(soup):
    
    try:
        ad_date = soup.find("div",{"class":"adInfo"}).getText().encode('ascii','ignore')
        ad_date = ad_date.replace("\r", "").replace("\n", "").replace("Posted:", "").strip()
        ad_date = datetime.strptime(ad_date, "%A, %B %d, %Y %I:%M %p")
        ad_date = ad_date.strftime("%Y-%m-%d %H:%M:%S")
 
        return ad_date
        
    except:
        return ""


def get_ad_id(url):
    
    try:
        ad_id = url.split('/')[5]
        return ad_id
        
    except:
        return ""


def get_category(url):
    
    try:
        category = url.split('/')[3]
        return category
        
    except:
        return ""


def get_site_id(url):

    site_id = url.split('/')[2].split('.')[0]
    return site_id


def get_other_ads(soup):
    
    try:
        other_ads = [element.a["href"] for element in soup.find("div",{"id":"OtherAdsByThisUser"}).findAll("div",{"class":"cat"})]
        other_ads = ";".join(other_ads)
        return other_ads
        
    except:
        return ""


def phone_extract(post):

    # define what punctuation is 
    #punct = '!"#$%&\'()*+,-./:;<=>?@[\\]^_`{|}~'
    
    # exclude $ from punctation, as sometimes a price is listed after phone number and I don't want to lump together
    punct = '!"#%&\'()*+,-./:;<=>?@[\\]^_`{|}~'
          
    # zap the post into lowercase
    post = post.lower()

    # remove all punctation
    post = ''.join(l for l in post if l not in punct)

    # remove all spaces
    post = post.replace(' ','')

    # create a dict of numeric words to replace with numbers
    numbers = {'zero'  : '0',
               'one'   : '1',
               'two'   : '2',
               'three' : '3',
               'four'  : '4',
               'five'  : '5',
               'six'   : '6',
               'seven' : '7',
               'eight' : '8',
               'nine'  : '9'}

    # look for each number spelled out in the post, and if found, replace with the numeric alternative
    for num in numbers:
        if num in post:
            post = post.replace(num, numbers[num])

    # extract all number sequences
    numbers = re.findall('\d+', post)

    # filter number strings to only include unique strings longer that are betweeb 7 and 11 characters in length
    phones = set([i for i in numbers if len(i) >= 7 and len(i) <= 11])
    
    # convert set to semicolon delimited
    if len(phones) > 0:
        phone_del = ";".join([i for i in phones])
        
    else:
        phone_del = ""

    return phone_del


def get_phone_number(soup):
    
    try:
        post_body = soup.find("div",{"class":"postingBody"}).getText().encode('ascii','ignore')
        post_body = post_body.replace("\r", "").replace("\n", "").strip()
        phone = phone_extract(post_body)
        return phone
    
    except:
        return ""


def get_locations(soup):
    
    try:
        locations = soup.find("div",text=re.compile("Location:")).getText().encode('ascii','ignore')
        locations = locations.replace("\r", "").replace("\n", "").replace("Location:", "").strip()
        return locations
    
    except:
        return ""


def get_post_body(soup):
    
    try:
        post_body = soup.find("div",{"class":"postingBody"}).getText().encode('ascii','ignore')
        post_body = post_body.replace("\r", "").replace("\n", "").strip()
        return post_body
    
    except:
        return ""


def get_poster_age(soup):
    
    try:
        poster_age = soup.find("p",{"class":"metaInfoDisplay"}).getText().encode('ascii','ignore')
        poster_age = poster_age.replace("\r", "").replace("\n", "").replace("Poster's age:", "").strip()
        return poster_age
    
    except:
        return ""


def get_post_title(soup):
    
    try:
        title = soup.find("div",{"id":"postingTitle"}).getText().encode('ascii','ignore')
        title = title.replace("\r", "").replace("\n", "").replace("Report Ad", "").strip()
        return title
        
    except:
        return ""


def load_city_state_as_dict(filename):

    # load the CSV of cities and states
    with open(filename, 'r') as f:
        data = list(csv.reader(f))

    # convert to a dict
    site_map = {}
    for line in data[1:]:
        site_map[line[0]] = {'city' : line[1],
                             'state' : line[2],
                             'region' : line[3],
                             'division' : line[4],
                             'url' : line[5]}

    return site_map


def clean_data(line):

    # load object into Beautiful Soup
    soup = bs(line['read'], "html.parser")

    # extract relevant fields
    row = {'ad_id'       : get_ad_id(line['url']),
           'ad_url'      : line['url'],
           'category'    : get_category(line['url']),
           'locations'   : get_locations(soup),
           'other_ads'   : get_other_ads(soup),
           'phone'       : get_phone_number(soup),
           'post_body'   : get_post_body(soup),
           'post_date'   : get_post_date(soup),
           'poster_age'  : get_poster_age(soup),
           'post_title'  : get_post_title(soup), 
           'scrape_date' : line['scrape_date'],
           'site_id'     : get_site_id(line['url']),
           'uniq_id'     : line['uniq_id']
          }
        
    return row


def conform_dbs(cur):

    # delete records that were posted before we collected the first ad
    cur.execute("""DELETE                        
                   FROM backpage
                   WHERE uniq_id IN (SELECT uniq_id 
                                     FROM backpage 
                                     WHERE post_date < (SELECT MIN(scrape_date) 
                                     FROM backpage));""")

    # delete reocrds that are in the clean data but not the raw data
    cur.execute("""DELETE 
                   FROM backpage
                   WHERE uniq_id NOT IN (SELECT uniq_id 
                                         FROM backpage_raw);""")


##### MAIN PROGRAM #####

@click.command()
@click.option('--batch_size', type=int, default=50, help='number of ads to load at a time (default=50)')
@click.option('--sleep_time', type=int, default=600, help='number of seconds to wait before checking for new data to load (default=600)')
def cli(batch_size, sleep_time):

    """ETL process for Backpage data"""

    # load CSV file with cities and states
    site_map = load_city_state_as_dict('./URLs.csv')

    # connect to the database
    try:
        conn = psycopg2.connect(database="postgres",
                                user="postgres",
                                password="apassword",
                                host="localhost")
        conn.autocommit = True
        cur = conn.cursor()
        logger.info("Successfully connected to the database")

    except:
        logger.warning("Cannot connect to database")
        exit
    
    # create table if it doesn't exists 
    cur.execute("""CREATE TABLE IF NOT EXISTS backpage ( 
                   id SERIAL PRIMARY KEY NOT NULL, 
                   ad_id VARCHAR,
                   ad_url VARCHAR,
                   category VARCHAR,
                   city VARCHAR,
                   division VARCHAR,
                   locations VARCHAR,
                   other_ads VARCHAR,
                   phone VARCHAR,
                   post_body VARCHAR,
                   post_date TIMESTAMP,
                   post_title VARCHAR,
                   poster_age VARCHAR,
                   region VARCHAR,
                   scrape_date TIMESTAMP,
                   site_id VARCHAR,
                   state VARCHAR,
                   uniq_id VARCHAR UNIQUE NOT NULL);""")

    # create a set of uniq_ids that cannot be loaded
    cannot_load = set()

    while True:
   
        # get the first 50 records that haven't been loaded
        cur.execute("""SELECT ad
                       FROM backpage_raw
                       WHERE uniq_id NOT IN (SELECT uniq_id
                                             FROM backpage)
                       LIMIT 50;""")


        # load the records into a list
        data = [record[0] for record in cur]

        # remove records that have been identifed as not able to load
        data = [line for line in data if line['uniq_id'] not in cannot_load]
                                                   
        # if there is something to load...
        if len(data) > 0:
           
            # transformed and load ads into the database
            for line in data:

                # run cleaning process
                clean_line = clean_data(line)

                # add in infomation from site map (city, state, region, division)
                clean_line['city'] = site_map[clean_line['site_id']]['city']
                clean_line['state'] = site_map[clean_line['site_id']]['state']
                clean_line['region'] = site_map[clean_line['site_id']]['region']
                clean_line['division'] = site_map[clean_line['site_id']]['division']

                # insert into database
                try:
                    cur.execute("""INSERT INTO backpage
                                   (ad_id, ad_url, category, city, division, locations, other_ads, phone, post_body, post_date, post_title, poster_age, region, scrape_date, site_id, state, uniq_id) 
                                   VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""", [clean_line[item] for item in sorted(clean_line.keys())])
                    #logger.info("New record inserted")

                except:
                    logger.warning("Cannot load record into backpage")
                    cannot_load.add(clean_line['uniq_id'])
                    pass


            logger.info("Successfully loaded another chunk in backpage")

        else:
   
            # conform databases and make sure the same records are in both
            conform_dbs(cur)

            # wait and see if there is something else to load
            logging.info("Waiting for new records...sleeping for {} seconds".format(sleep_time))
            sleep(sleep_time)

if __name__ == "__main__":
    cli()

