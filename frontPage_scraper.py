#!/usr/bin/env python

import socket
import socks
import click
import logging
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
import frontPage_etl as etl


# enable logging
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s %(levelname)s %(module)s - %(funcName)s: %(message)s',
                    datefmt="%Y-%m-%d %H:%M:%S")
logger = logging.getLogger()
logger.setLevel(logging.INFO)


##### HELPER FUNCTIONS ##### 

def getaddrinfo(*args):

    return [(socket.AF_INET, socket.SOCK_STREAM, 6, '', (args[0], args[1]))]


def enable_tor():

    socks.setdefaultproxy(socks.PROXY_TYPE_SOCKS5, '127.0.0.1', 9050)
    socket.socket = socks.socksocket

    # check IP address
    socket.getaddrinfo = getaddrinfo
    r = urllib.urlopen('http://my-ip.herokuapp.com').read()
    logger.info("TOR enabled: {}".format(r.split('"')[3]))


def create_output_dir(path):

    # get current datetime
    dt = datetime.now().strftime("%Y%m%d%H%M")
    
    # combine path and datetime for absolute path
    out_path = path + "/" + "backpage_" + dt
    
    # check if the path already exists
    if os.path.exists(out_path):
        logger.info("Output directory already exists")
    
    # if it doesn't exsits, create it
    else:
        os.makedirs(out_path)
        logger.info("Output directory created")
        
    return out_path


def get_urls(landing_page, sleep_time):
    
    # extract city and category from URL
    city = landing_page.split("/")[2].split(".")[0]
    category = landing_page.split("/")[3]
    
    # create a list to hold the URLS
    urls = []
    
    # go through each page and find the links to all the ads
    page_num = 1
    
    while True:
    
        # create the URL
        url = landing_page + "?page=" + str(page_num)
        
        # try to open the URL
        while True:
            try:
                soup = bs(urllib2.urlopen(url), "html.parser")
                break

            except:
                logger.info("Unable to get URLs for {} - {}".format(city, category))
                enable_tor()
                logger.info("Sleeping for {} seconds before trying again".format(sleep_time))
                sleep(sleep_time)
        
        # look for the links
        if "No matches found." not in soup.get_text():

            # find all the links to the other ads    
            for element in soup.findAll("div", {"class" : re.compile("cat*")}):
                urls.append(element.a["href"])
                
            # log event
            #logger.info("Success: {}".format(url))

            # increment page number by 1
            page_num += 1
            
            # wait for a random amount of time
            sleep(random() * 2)
        
        # if no links are found, stop
        else:
            #logger.info("No more pages available")
            break
            
    logger.info("Number of ads in {} - {}: {}".format(city, category, len(urls)))
    
    return urls


def fetch_imgs(url, i, img_path):
    
    ad_imgs = []
    img_num = 0
    
    # load URL as bs objects
    soup = bs(urllib2.urlopen(url), "html.parser")
    
    # get all images and their links
    images = [img for img in soup.findAll('img')]
    image_links = [each.get('src') for each in images]
    
    # save each images to local dir
    for img in image_links:
        
        # create a filename
        filename = img_path + "/" + str(i) + "-" + str(img_num)
        ad_imgs.append(filename)
            
        # save down the image
        urllib.urlretrieve(img, filename)
            
        # advance the filename counter
        img_num += 1
            
    return ad_imgs


def store_html_in_dict(url):
    
    response = urllib2.urlopen(url)
    
    data = {'scrape_date' : datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            'code' : response.code,
            'url'  : response.url,
            'read' : response.read()}
    
    return data


def create_uniq_id(data):

    #  load object into Beautiful Soup
    soup = bs(data['read'], "html.parser")

    # parse fields to create unique id
    ad_id = etl.get_ad_id(data)    
    city = etl.get_city(data)
    category = etl.get_category(data)
    post_date = etl.get_ad_date(soup)

    # combine fields into unique id
    uniq_id = post_date + "-" + ad_id + "-" + city + "-" + category

    return uniq_id


##### MAIN PROGRAM #####


@click.command()
@click.option('--sleep_time', type=int, default=23, help = 'Number of seconds to sleep if there is a error getting the URLs (default=23)')
@click.option('--get_imgs/--no_imgs', default = False, help = 'Options to download all images from the ads (default: no_imgs)')
@click.option('--category_file', default = './categories.txt', help = 'File for TXT file of categories to scrape (default: ./categories.txt')
@click.option('--city_file', default = './cities.txt', help = 'File for TXT file of cities to scrape (default: ./cities.txt')
def cli(sleep_time, get_imgs, category_file, city_file):

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

    # if you want to get images, create directory to store images
    if get_imgs is True:
        path = "/home/elmer/frontPage"
        img_path = create_output_dir(path)

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
        for i, url in enumerate(ad_urls):

            # try each url, if you doesn't work, skip it
            try:
            
                # store HTML data in dict
                ad = store_html_in_dict(url)

                # create unique_id for ad
                uniq_id = create_uniq_id(ad)
                ad['uniq_id'] = uniq_id

                # if you want to collect images...
                if get_imgs is True:

                    # store images in local directory
                    ad_imgs = fetch_imgs(url, i, img_path)

                    # add image paths to ad data
                    ad['img_paths'] = ad_imgs

                    # count the number of images and add to ad data
                    ad['num_imgs'] = len(ad_imgs)

                try:
                
                    # insert ad data into table
                    cur.execute("INSERT INTO backpage_raw (uniq_id, ad) VALUES (%s, %s)", [uniq_id, json.dumps(ad)])
            
                except:
                    pass

            except:
                logger.info("Skipped: {}".format(url))
                enable_tor()
                pass

            # sleep for a hot second...
            sleep(random())

        logger.info("Number of ads collected in {} - {}: {}".format(line[0], line[1], i + 1))


if __name__ == "__main__":
    cli()
