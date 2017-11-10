#!/usr/bin/env python
import click
import logging
import psycopg2


# enable logging
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s %(levelname)s %(module)s - %(funcName)s: %(message)s',
                    datefmt="%Y-%m-%d %H:%M:%S")
logger = logging.getLogger()
logger.setLevel(logging.INFO)



##### MAIN PROGRAM #####
@click.command()
def cli():

    """ETL process for create central fact table"""

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
    logger.info("Cannot connect to database")

    # load the id and phones from the ad data
    cur.execute("""SELECT id, phone
                   FROM backpage;""")
    data = [record for record in cur]

    # create a table to hold the unique phones
    cur.execute("""CREATE TABLE IF NOT EXISTS backpage_phone ( 
                   id SERIAL PRIMARY KEY NOT NULL, 
                   phone VARCHAR UNIQUE NOT NULL);""")

    # load the phones into the phone table
    for i in data:
    
        # loop over the phones found in each ad...
        phones = i[1].split(';')
        for p in phones:
    
            # if the phone number is not blank...
            if p != '':
            
                # try to load the phone number into the table
                try:

                    # add to the phone table
                    cur.execute("""INSERT INTO backpage_phone
                                   (phone) 
                                    VALUES (%s);""", [p])
        
                # if it won't load, it might already be in there, so move on
                except:
                    pass

    # create a table to hold the phone_to_ad relationships
    cur.execute("""CREATE TABLE IF NOT EXISTS backpage_adphone(
                   PRIMARY KEY(phone_id, ad_id),
                   phone_id integer NOT NULL,
                   ad_id integer NOT NULL);""")

    # get the phone numbers and ids from the phone table
    cur.execute("""SELECT id, phone
                    FROM backpage_phone""")

    # store the query results in a dict (phone=key, id=value)
    all_phones = {}
    for record in cur:
        all_phones[record[1]] = record[0]

    # load the phone id and ad id into the phone_to_ad table
    for i in data:
    
        # loop over the phones found in each ad...
        phones = i[1].split(';')
        for p in phones:
    
            # if the phone number is not blank...
            if p != '':
            
                phone_id = all_phones[p]
                ad_id = i[0]
            
                # try to load the phone number into the table
                try:

                    # add to the phone table
                    cur.execute("""INSERT INTO backpage_adphone
                                   (phone_id, ad_id) 
                                   VALUES (%s, %s);""", [phone_id, ad_id])
        
                # if it won't load, it might already be in there, so move on
                except:
                    pass
