
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




# # Define Functions to Extract Fields

# In[24]:

def get_ad_date(soup):
    
    try:
        ad_date = soup.find("div",{"class":"adInfo"}).getText().encode('ascii','ignore')
        ad_date = ad_date.replace("\r", "").replace("\n", "").replace("Posted:", "").strip()
        ad_date = datetime.strptime(ad_date, "%A, %B %d, %Y %I:%M %p")
        ad_date = ad_date.strftime("%Y-%m-%d %H:%M:%S")
        return ad_date
        
    except:
        return ""


# In[25]:

def get_ad_id(line):
    
    try:
        url = line['url']
        ad_id = url.split('/')[5]
        return ad_id
        
    except:
        return ""


# In[26]:

def get_category(line):
    
    try:
        url = line['url']
        category = url.split('/')[3]
        return category
        
    except:
        return ""


# In[27]:

def get_city(line):
    
    try:
        url = line['url']
        city = url.split('/')[2].split('.')[0]
        return city
        
    except:
        return ""


# In[28]:

def get_other_ads(soup):
    
    try:
        other_ads = [element.a["href"] for element in soup.find("div",{"id":"OtherAdsByThisUser"}).findAll("div",{"class":"cat"})]
        other_ads = ";".join(other_ads)
        return other_ads
        
    except:
        return ""


# In[29]:

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


# In[30]:

def get_phone_number(soup):
    
    try:
        post_body = soup.find("div",{"class":"postingBody"}).getText().encode('ascii','ignore')
        post_body = post_body.replace("\r", "").replace("\n", "").strip()
        phone = phone_extract(post_body)
        return phone
    
    except:
        ""


# In[31]:

def get_locations(soup):
    
    try:
        locations = soup.find("div",text=re.compile("Location:")).getText().encode('ascii','ignore')
        locations = locations.replace("\r", "").replace("\n", "").replace("Location:", "").strip()
        return locations
    
    except:
        return ""


# In[11]:

def get_image_paths(line):
    
    try:
        return line['img_paths']
        
    except:
        return ""


# In[12]:

def get_num_images(line):
    
    try:
        return line['num_imgs']
        
    except:
        return ""


# In[13]:

def get_post_body(soup):
    
    try:
        post_body = soup.find("div",{"class":"postingBody"}).getText().encode('ascii','ignore')
        post_body = post_body.replace("\r", "").replace("\n", "").strip()
        return post_body
    
    except:
        return ""


# In[14]:

def get_poster_age(soup):
    
    try:
        poster_age = soup.find("p",{"class":"metaInfoDisplay"}).getText().encode('ascii','ignore')
        poster_age = poster_age.replace("\r", "").replace("\n", "").replace("Poster's age:", "").strip()
        return poster_age
    
    except:
        return ""


# In[15]:

def get_post_title(soup):
    
    try:
        title = soup.find("div",{"id":"postingTitle"}).getText().encode('ascii','ignore')
        title = title.replace("\r", "").replace("\n", "").replace("Report Ad", "").strip()
        return title
        
    except:
        return ""


# In[16]:

def clean_data(line):
    
    # if the response was successful
    if line['code'] == 200:
    
        #  load object into Beautiful Soup
        soup = bs(line['read'], "lxml")

        # extract relevant fields
        row = {'ad_date' : get_ad_date(soup),
               'ad_id' : str(get_ad_id(line)),
               'category' : str(get_category(line)),
               'city' : str(get_city(line)),
               'image_paths' : str(get_image_paths(line)),
               'locations' : get_locations(soup),
               'num_images' : str(get_num_images(line)),
               'other_ads' : get_other_ads(soup),
               'phone' : get_phone_number(soup),
               'post_body' : get_post_body(soup),
               'poster_age' : get_poster_age(soup),
               'post_title' : get_post_title(soup), 
               'scrape_date' : str(line['scrape_date']),
               'url' : str(line['url'])
              }
        
        return row

    else:
        
        logger.info("Issue in cleaning data")
        pass


# In[ ]:




# # Run Cleaning Process and Insert Clean Data

# In[34]:

def cli():

    """ETL process for Backpage data"""

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

    
    # create table if it doesn't exists 
    cur.execute("""CREATE TABLE IF NOT EXISTS backpage ( 
                   id SERIAL PRIMARY KEY NOT NULL,
                   ad_date TIMESTAMP,
                   ad_id VARCHAR,
                   category VARCHAR,
                   city VARCHAR,
                   image_paths VARCHAR,
                   locations VARCHAR,
                   num_images VARCHAR,
                   other_ads VARCHAR,
                   phone VARCHAR,
                   post_body VARCHAR,
                   post_title VARCHAR,
                   poster_age VARCHAR,
                   scrape_date TIMESTAMP,
                   url VARCHAR);""")
    
    # query the database and store the results
    try:
        cur.execute("""SELECT ad FROM backpage_raw""") 
        data = [record[0] for record in cur]
        logger.info("Loaded %s records from backpage_raw", len(data)) 

    except:
        logger.warning("Unable to query backpage")


    # load transformed ads into the database
    for line in data:

        # run cleaning process
        clean_line = clean_data(line)

        # insert into database
        try:
            cur.execute("INSERT INTO backpage (ad_date, ad_id, category, city, image_paths, locations, num_images, other_ads, phone, post_body, post_title, poster_age, scrape_date, url) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)", [clean_line[item] for item in sorted(clean_line.keys())])

        except:
           #logger.warning("Cannot load record into backpage")
            pass


    logger.info("Successfully loaded records in backpage")


# In[35]:

cli()


# In[ ]:



