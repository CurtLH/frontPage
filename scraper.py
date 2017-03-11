
# coding: utf-8

# # Customize Enviornment

# In[1]:

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
from pprint import pprint


# In[2]:

# enable logging
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s %(levelname)s %(module)s - %(funcName)s: %(message)s',
                    datefmt="%Y-%m-%d %H:%M:%S")
logger = logging.getLogger()
logger.setLevel(logging.INFO)


# In[ ]:




# # Establish Database Connection

# In[3]:

# connect to the database
conn = psycopg2.connect(database="postgres",
                        user="postgres",
                        password="apassword",
                        host="localhost")

# enable autocommit
conn.autocommit = True

# define the cursor to be able to write to the database
cur = conn.cursor()


# In[4]:

# create the table if it doesn't exist
cur.execute("""CREATE TABLE IF NOT EXISTS backpage_raw 
               (id SERIAL PRIMARY KEY NOT NULL, 
               ad JSONB)""") 


# In[ ]:




# # Functions

# In[5]:

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


# In[6]:

def create_urls_to_scrape():
    
    import params
    
    # load categories and cites from parameters file
    categories = params.categories
    cities = params.cities
    
    # create all possible urls
    landing_urls = []
    for category in categories:
        for city in cities:
            url = "http://" + city + ".backpage.com/" + category + "/"
            landing_urls.append(url)
            
    logging.info("Number of landing pages to scrape: {}".format(len(landing_urls)))
    
    return landing_urls


# In[8]:

def get_urls(landing_page):
    
    # extract city and category from URL
    city = landing_page.split("/")[2].split(".")[0]
    category = landing_page.split("/")[3]
    
    # create a list to hold the URLS
    urls = []
    
    # go through each page and find the links to all the ads
    page_num = 1
    
    while True:
    
        # open the URL
        url = landing_page + "?page=" + str(page_num)
        soup = bs(urllib2.urlopen(url), "lxml")
        
        # look for the links
        if "No matches found." not in soup.get_text():

            # find all the links to the other ads    
            for element in soup.findAll("div", {"class" : re.compile("cat*")}):
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
            
    logger.info("Number of URLs in {} - {}: {}".format(city, category, len(urls)))
    
    return urls


# In[9]:

def fetch_imgs(url, i, img_path):
    
    ad_imgs = []
    img_num = 0
    
    # load URL as bs objects
    soup = bs(urllib2.urlopen(url), "lxml")
    
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


# In[10]:

def store_html_in_dict(url):
    
    response = urllib2.urlopen(url)
    
    data = {'scrape_date' : datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            'code' : response.code,
            'url'  : response.url,
            'read' : response.read()}
    
    return data


# In[11]:

def iter_urls(urls, img_path, get_imgs = True):
    
    for i, url in enumerate(urls):
        
        # store HTML data in dict
        ad = store_html_in_dict(url)
        
        if get_imgs is True:
            
            # store images in local directory
            ad_imgs = fetch_imgs(url, i, img_path)
        
            # add image paths to data
            ad['img_paths'] = ad_imgs

            # count the number of images
            ad['num_imgs'] = len(ad_imgs)
        
        # insert ad into table
        cur.execute("INSERT INTO backpage_raw (ad) VALUES (%s)", [json.dumps(ad)])
        
        # sleep for a hot minute...
        sleep(1)
        
    logger.info("Number of ads collected: {}".format(i + 1))


# In[12]:

def scrape_city_category(landing_page = "", get_imgs = False):
    
    # set blank img_path
    img_path = ""
    
    # if you want to scrape images as well
    if get_imgs is True:
        
        # estalish output directory
        path = "/home/curtis/Github/Analytic-Projects/state_police/backpage"
        img_path = create_output_dir(path)
        
    # get URLS to scrape
    urls = get_urls(landing_page)
        
    # iterate through urls and store data
    iter_urls(urls, img_path, get_imgs = get_imgs)


# In[13]:

def scrape_all(get_imgs = False):
    
    # define urls to crawl
    landing_urls = create_urls_to_scrape()
    
    # scrape each city/category
    for url in landing_urls:
        scrape_city_category(landing_page=url, get_imgs=get_imgs)


# In[ ]:




# In[ ]:




# # Main Code Body

# In[14]:

scrape_all(get_imgs=False)


# In[ ]:




# In[ ]:



