#!/usr/bin/env python

import socket
import socks
import click
import logging
from datetime import datetime
from bs4 import BeautifulSoup as bs
import re
import urllib2
import psycopg2
import json
from random import random
from time import sleep
import etl_process as etl
from functools import wraps
import time

# enable logging
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s %(levelname)s %(module)s - %(funcName)s: %(message)s',
                    datefmt="%Y-%m-%d %H:%M:%S")
logger = logging.getLogger()
logger.setLevel(logging.INFO)


# HELPER FUNCTIONS

def getaddrinfo(*args):

    return [(socket.AF_INET, socket.SOCK_STREAM, 6, '', (args[0], args[1]))]


def enable_tor():

    socks.setdefaultproxy(socks.PROXY_TYPE_SOCKS5, '127.0.0.1', 9050)
    socket.socket = socks.socksocket

    # log new IP address
    socket.getaddrinfo = getaddrinfo
    r = urllib2.urlopen('http://my-ip.herokuapp.com').read()
    logger.info("TOR enabled: {}".format(r.split('"')[3]))


def get_urls(landing_page, sleep_time):

    # extract city and category from URL
    city = landing_page.split("/")[2].split(".")[0]
    category = landing_page.split("/")[3]

    # create a list to hold the URLS
    urls = []

    # start at page #1
    page_num = 1

    # go through each page and find the links to all the ads
    while True:

        # create the URL
        url = landing_page + "?page=" + str(page_num)

        # try to open the URL
        while True:
            try:
                soup = bs(urllib2.urlopen(url), "html.parser")
                break

            # if it doesn't open, take a break, refresh IP, and try again
            except:
                logger.info("Unable to get URLs for {} - {}".format(city, category))
                logger.info("Sleeping for {} seconds before trying again".format(sleep_time))
                sleep(sleep_time)
                enable_tor()

        # look for the links
        if "No matches found." not in soup.get_text():

            # find all the links to the other ads
            for element in soup.findAll("div", {"class": re.compile("cat*")}):
                urls.append(element.a["href"])

            # log event
            logger.info("Success: {}".format(url))

            # increment page number by 1
            page_num += 1

            # wait for a random amount of time
            sleep(random() * 2)

        # if no links are found, stop
        else:
            logger.info("No more pages available")
            break

    # log total number of URLs from each city/category
    logger.info("Number of ads in {} - {}: {}".format(city, category, len(urls)))

    return urls

def retry(ExceptionToCheck, tries=4, delay=30, backoff=2):

    """http://www.saltycrane.com/blog/2009/11/trying-out-retry-decorator-python/"""

    def deco_retry(f):

        @wraps(f)
        def f_retry(*args, **kwargs):

            mtries, mdelay = tries, delay

            while mtries > 1:

                try:
                    return f(*args, **kwargs)

                except ExceptionToCheck, e:
                    logger.info("%s, Retrying in %d seconds..." % (str(e), mdelay))

                    time.sleep(mdelay)
                    mtries -= 1
                    mdelay *= backoff

            return f(*args, **kwargs)

        return f_retry  # true decorator

    return deco_retry


@retry(urllib2.URLError, tries=4, delay=30, backoff=2)
def urlopen_with_retry(url):

    # open URL and return the response item
    response = urllib2.urlopen(url)

    return response


def store_html_in_dict(response):

    # bundle the response object with other response attributes
    data = {'scrape_date': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            'code': response.code,
            'url': response.url,
            'read': response.read()}

    return data


def create_uniq_id(data):

    #  load object into Beautiful Soup
    soup = bs(data['read'], "html.parser")

    # parse fields to create unique id
    ad_id = etl.get_ad_id(data['url'])
    site_id = etl.get_site_id(data['url'])
    category = etl.get_category(data['url'])
    post_date = etl.get_post_date(soup)

    # combine fields into unique id
    uniq_id = post_date + "-" + ad_id + "-" + site_id + "-" + category

    return uniq_id


# MAIN PROGRAM
@click.command()
@click.option('--sleep_time', type=int, default=60, help='Number of seconds to sleep if there is a error getting the URLs (default=60)')
@click.option('--category_file', default='./default_categories.txt', help='File for TXT file of categories to scrape (default: ./default_categories.txt')
@click.option('--city_file', default='./default_cities.txt', help='File for TXT file of cities to scrape (default: ./default_cities.txt')
def cli(sleep_time, category_file, city_file):

    """Web scraper for collecting ad information"""

    # use TOR to mask ip address
    enable_tor()

    try:

        # connect to the database
        conn = psycopg2.connect(database="postgres",
                                user="postgres",
                                password="apassword",
                                host="localhost")

        # enable autocommit
        conn.autocommit = True

        # define the cursor to be able to write to the database
        cur = conn.cursor()
        logging.info("Successfully connected to the database")

    except:
        logging.info("Unable to connect to the database")

    # create the table if it doesn't exist
    cur.execute("""CREATE TABLE IF NOT EXISTS backpage_raw
                   (id SERIAL PRIMARY KEY NOT NULL,
                    uniq_id VARCHAR UNIQUE NOT NULL,
                    ad JSONB)""")

    # load cities and categories to scrape
    categories = set(line.lower().strip() for line in open(category_file))
    cities = set(line.lower().strip() for line in open(city_file))

    # create all possible city/category combinations
    city_category = []
    for city in cities:
        for category in categories:
            city_category.append((city, category))

    # for each landing page (i.e.: Baton Rouge WomenSeekMen)
    for line in city_category:

        # create url to get the links for the ads
        base_url = "http://" + line[0] + ".backpage.com/" + line[1] + "/"

        # go through all pages to get links to for all ads for that city/cateory
        ad_urls = get_urls(base_url, sleep_time)

        # go to each ad and store content
        for url in ad_urls:

            # query URL  
            response = urlopen_with_retry(url)

            # try each url and store HTML data in dict
            ad = store_html_in_dict(response)

            # if the results did not come back
            if ad is not False:

                # create unique_id for ad
                uniq_id = create_uniq_id(ad)

                # add the unique id to the dict
                ad['uniq_id'] = uniq_id

                # convert the dict to JSON object
                ad_json = json.dumps(ad)

                # try to insert the JSON object
                try:
                    cur.execute("INSERT INTO backpage_raw (uniq_id, ad) VALUES (%s, %s)", [uniq_id, ad_json])
                    logger.info("New record inserted: {}".format(uniq_id))

                # if it's not successful, log the event and move on
                except:
                    #logger.info("Record already exists in the database: {}".format(uniq_id))
                    pass

if __name__ == "__main__":
    cli()
