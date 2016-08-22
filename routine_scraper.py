#!/usr/bin/python
import os
import sys
import redis
import scrapUtils
import MySQLdb
import logging
from logging.handlers import RotatingFileHandler

INSERT_EVENT = "INSERT INTO celebrities_event SET celebrity_id=%s, event=%s, timestamp=UTC_TIMESTAMP(), user_id=2;"
RUNNING_FILE="/home/reminiz/RUNNING.signal"
LOG_FILE = "/home/reminiz/log_scrap.log"

def connectDB():
    conn=MySQLdb.connect(host="rcwd-sql.cloudapp.net", db="rcwd", user="django", passwd="GpqozK0q9dUE6jG")
    cur = conn.cursor()
    return conn, cur

def disconnectDB(conn, cur):
    conn.commit()
    cur.close()
    conn.close()

def scrap_celebrity(celeb_id, searchQuery):
    # Notifying the database event
    (conn, cur) = connectDB()
    cur.execute(INSERT_EVENT, (celeb_id, 2))
    disconnectDB(conn, cur)
    logger.info(INSERT_EVENT % (celeb_id, 2))

    # scraping the images proposed by google image
    logger.info("scraping the images proposed by google image")
    scrapUtils.prepareScraping(celeb_id)

    # crawl the google image path
    logger.info("crawl the google image path")
    scrapUtils.crawlGoogle(celeb_id, searchQuery)

    # parse the web page
    logger.info("parse the web page")
    scrapUtils.parsePage(celeb_id)

    # dl the files on a local repo
    logger.info("dl the files on a local repo")
    scrapUtils.dlFiles(celeb_id)

    # upload everything on the s3
    logger.info("upload everything on the s3")
    scrapUtils.uploadScrap(celeb_id)

    # clean scrap
    logger.info("clean scrap")
    scrapUtils.cleanScrap(celeb_id)

    # Exiting the database, writing the DB transactions
    logger.info(INSERT_EVENT % (celeb_id, 3))
    (conn, cur) = connectDB()
    cur.execute(INSERT_EVENT, (celeb_id, 3))
    disconnectDB(conn, cur)


# logger object
# un fichier en mode 'append', avec 1 backup et une taille max de 1Mo
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)
formatter = logging.Formatter('[%(asctime)s] %(levelname)s :: %(message)s')
file_handler = RotatingFileHandler(LOG_FILE, 'a', 1000000, 1)
file_handler.setLevel(logging.DEBUG)
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

steam_handler = logging.StreamHandler()
steam_handler.setLevel(logging.DEBUG)
logger.addHandler(steam_handler)

## Don't execute the script if already running
if os.path.exists(RUNNING_FILE):
    logger.info("Already running.")
    sys.exit()
else:
    open(RUNNING_FILE, 'a').close()

## Loop on the celebrities.
r = redis.StrictRedis(host='40.118.3.19', port=6379, password='YoBGdu93')
celebrity = r.lpop('to_scrap')

while(celebrity):
    (celeb_id, name, searchQuery) = celebrity.split(';')

    (conn, cur) = connectDB()
    cur.execute(INSERT_EVENT, (celeb_id, 4))
    disconnectDB(conn, cur)

    scrap_celebrity(celeb_id, searchQuery)

    r = redis.StrictRedis(host='40.118.3.19', port=6379, password='YoBGdu93')
    celebrity = r.lpop('to_scrap')

# Remove the running file
logger.info("List is empty, exiting.")
os.remove(RUNNING_FILE)
