#!/usr/bin/python
import os
import sys
import preprocessUtils
import redis
import MySQLdb
import logging
from logging.handlers import RotatingFileHandler

INSERT_EVENT = "INSERT INTO celebrities_event SET celebrity_id=%s, event=%s, timestamp=UTC_TIMESTAMP(), user_id=2;"
RUNNING_FILE = "/disk2/tonio/scrap_type_2/RUNNING.signal"

def connectDB():
    conn=MySQLdb.connect(host="rcwd-sql.cloudapp.net", db="rcwd", user="django", passwd="GpqozK0q9dUE6jG")
    cur = conn.cursor(MySQLdb.cursors.DictCursor)
    return conn, cur

def disconnectDB(conn, cur):
    conn.commit()
    cur.close()
    conn.close()

def preprocess_celebrity(celebrity, celeb_id):
    # prepare preprocessing
    logger.info("prepare preprocessing for celebrity %s" % celebrity)
    preprocessUtils.preparePreprocessing(celeb_id)

    # gets data from s3
    logger.info("gets data from s3")
    dirHasImages = preprocessUtils.downloadScrap(celeb_id)

    if dirHasImages:
        # detects all faces and extracts the right crops
        logger.info("detects all faces and extracts the right crops")
        (conn, cur) = connectDB()
        preprocessUtils.detectFaces(celeb_id, conn, cur, logger)
        disconnectDB(conn, cur)

        # ranks the images and create the batches
        logger.info("ranks the images and create the batches")
        (conn, cur) = connectDB()
        preprocessUtils.rankFaces(celeb_id, conn, cur, logger)
        disconnectDB(conn, cur)

        # upload everything to s3
        logger.info("upload everything to s3")
        preprocessUtils.uploadPreprocessing(celeb_id)

        r = redis.StrictRedis(host='40.118.3.19', port=6379, password='YoBGdu93')
        r.rpush('to_annot', celebrity)
        logger.info("Sent to Redis 'to_annot' list.")
        (conn, cur) = connectDB()
        cur.execute(INSERT_EVENT, (celeb_id, 6))
        disconnectDB(conn, cur)

    else:
        logger.warning("No image found for celebrity %s" % celebrity)
        r = redis.StrictRedis(host='40.118.3.19', port=6379, password='YoBGdu93')
        r.rpush('to_scrap2', celebrity)
        logger.warning("Sent to Redis 'to_scrap' list.")

        (conn, cur) = connectDB()
        cur.execute(INSERT_EVENT, (celeb_id, 5))
        disconnectDB(conn, cur)

    # clean preprocessing
    logger.info("clean preprocessing")
    preprocessUtils.cleanPreprocessing(celeb_id)
    logger.info("End of celebrity %s preprocessing. Success: %s." % (celebrity, dirHasImages))


# logger object
# un fichier en mode 'append', avec 1 backup et une taille max de 1Mo
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)
formatter = logging.Formatter('[%(asctime)s] %(levelname)s :: %(message)s')
file_handler = RotatingFileHandler('/disk2/tonio/scrap_type_2/log_preproc.log', 'a', 1000000, 1)
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
celebrity = r.lpop('to_preproc')

while(celebrity):
    celeb_id = celebrity.split(';')[0]

    (conn, cur) = connectDB()
    cur.execute(INSERT_EVENT, (celeb_id, 4))
    disconnectDB(conn, cur)

    preprocess_celebrity(celebrity, celeb_id)

    r = redis.StrictRedis(host='40.118.3.19', port=6379, password='YoBGdu93')
    celebrity = r.lpop('to_preproc')

# Remove the running file
logger.info("List is empty, exiting.")
os.remove(RUNNING_FILE)
