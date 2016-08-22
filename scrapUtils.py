import os
import sys
import uuid
import shutil
import urllib
import re
import io
import MySQLdb

from collections import OrderedDict
import subprocess
from lxml import html

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

MOUNTDATA = "/home/reminiz/scraping/mnt/data"
IMAGE_INSERT = "INSERT INTO images (url, local_path, s3_path, celebrity_id) VALUES (%s, %s, %s, %s)"
UPDATE_CELEB = "UPDATE celebrity SET step=%s, state=%s WHERE celebrity.id = %s"

def connectDB():
    conn=MySQLdb.connect(host="rcwd-sql.cloudapp.net", db="rcwd", user="django", passwd="GpqozK0q9dUE6jG")
    cur = conn.cursor()
    return conn, cur
def disconnectDB(conn, cur):
    conn.commit()
    cur.close()
    conn.close()

def updateCelebStatus(celeb_id, (step, state)):
    # updates celeb status
    (conn, cur) = connectDB()
    cur.execute(UPDATE_CELEB, (step, state, celeb_id))
    disconnectDB(conn, cur)

def prepareScraping(celeb_id):
    # creates the folder architecture in MOUNTDATA :
    # MOUNTDATA/celeb_id
    # MOUNTDATA/celeb_id/images
    # MOUNTDATA/celeb_id/crops
    outputDir = '%s/%s' % (MOUNTDATA, celeb_id)
    if os.path.isdir(outputDir):
        shutil.rmtree(outputDir)
    os.mkdir(outputDir)
    os.mkdir(os.path.join(outputDir, 'images'))
    return

def crawlGoogle(celeb_id, searchQuery):
    # uses phantomjs to scrap the google image page
    # saves page content on MOUNTDATA/celeb_id/page.html
    updateCelebStatus(celeb_id, (0, 1))

    outputDir = '%s/%s' % (MOUNTDATA, celeb_id)
    outputFilename = os.path.join(outputDir, 'page.html')
    cmd = ['phantomjs', '/home/reminiz/scraping/tools/scrollingGoogleNested.js', outputFilename, '+'.join(searchQuery.split())]
    # '--proxy=127.0.0.1:9050', '--proxy-type=socks5',

    p = subprocess.Popen(cmd)
    out, err = p.communicate()

    updateCelebStatus(celeb_id, (0, 2))
    updateCelebStatus(celeb_id, (1, 0))

def parsePage(celeb_id):
    # parses the google image and saves results into
    # MOUNTDATA/celeb_id/list_files.txt
    updateCelebStatus(celeb_id, (1, 1))

    outputDir = '%s/%s' % (MOUNTDATA, celeb_id)
    pageFile = os.path.join(outputDir, 'page.html')
    listImages = os.path.join(outputDir, 'list_files.txt')

    filesToDownload = parseGoogleWebPage(pageFile)
    writeUrls(filesToDownload, listImages)

    updateCelebStatus(celeb_id, (1, 2))
    updateCelebStatus(celeb_id, (2, 0))

def dlFiles(celeb_id):
    # downloads the files into MOUNTDATA/celeb_id/images
    updateCelebStatus(celeb_id, (2, 1))

    outputDir = '%s/%s' % (MOUNTDATA, celeb_id)
    list_files = os.path.join(outputDir, 'list_files.txt')
    cmd = ['aria2c',
            '--input-file', '%s/%s/list_files.txt' % (MOUNTDATA, celeb_id),
            '--connect-timeout', '5',
            '--timeout', '5',
            '--dir', '%s/%s/images' % (MOUNTDATA, celeb_id),
            '--log', '%s/%s/dl.log' % (MOUNTDATA, celeb_id)]

    # dowloading the images
    p = subprocess.Popen(cmd)
    out, err = p.communicate()

    # inserting the images into the db
    urls = parseListFiles(list_files)
    insertImagesToDb(celeb_id, urls)

    updateCelebStatus(celeb_id, (2, 2))
    updateCelebStatus(celeb_id, (3, 0))

def uploadScrap(celeb_id):
    # empties the s3 directory to clean it
    s3FilesTODelete = 's3://rcwd/data/%s/' % celeb_id
    cmd = ['s3cmd', 'del', '--verbose', '--recursive', s3FilesTODelete]
    p = subprocess.Popen(cmd)
    out, err = p.communicate()
    # sends data to the s3
    pathToS3 = 's3://rcwd/data/'
    outputDir = '%s/%s' % (MOUNTDATA, celeb_id)
    cmd = ['s3cmd', 'put', '--verbose', '--recursive', outputDir, pathToS3]
    p = subprocess.Popen(cmd)
    out, err = p.communicate()
    return

def cleanScrap(celeb_id):
    # deletes the folder on mnt
    outputDir = '%s/%s' % (MOUNTDATA, celeb_id)
    if os.path.isdir(outputDir):
        shutil.rmtree(outputDir)


### UTILS ###


def parseGoogleWebPage(pageFile):
    # parses google html page
    # returns ordered list of urls
    imgUrls = []
    with io.open(pageFile, 'r', encoding='utf8') as file_id:
        page = file_id.read().strip('\n')
        tree = html.fromstring(page)
        images = tree.xpath('//div[@class="rg_di rg_bx rg_el ivg-i"]')
        for count, image in enumerate(images):
            try:
                a = image.getchildren()[0]
                href = a.attrib['href']
                m = re.search("(?<=imgurl=)(.*?)(?=&imgrefurl)", href)
                imgUrl = m.group(0)
                imgUrl = urllib.unquote(imgUrl).decode('utf8')
                imgUrls.append(imgUrl)
            except:
                print 'not existing image'
    return imgUrls

def writeUrls(imgUrls, filename):
    # saves imgages url into a file for aria2c
    # format :
    # imgurl\n
    # \tout=image_id\n
    with open(filename, 'w') as file_id:
        for count, imgUrl in enumerate(imgUrls):
            file_id.write('%s\n\tout=%s\n' % (imgUrl, uuid.uuid4().hex))
            # if count > 20:
                # break


def parseListFiles(filename):
    # parses the content of the list_file.txt
    # takes as input a file with format "url\t\nout=filename\n"
    # and outputs dict with format to_return[filename] = url
    with open(filename) as file_id:
        content = file_id.read().strip()
    splits = content.split('\n')
    urls = splits[0::2]
    filenames = [s.strip('\tout=') for s in splits[1::2]]
    to_return = OrderedDict()
    for filename, url in zip(filenames, urls):
        # print filename, url
        to_return[filename] = url
    return to_return

def insertImagesToDb(celeb_id, urls):
    outputDir = '%s/%s' % (MOUNTDATA, celeb_id)
    outputS3 = 'rcwd.s3.amazonaws.com/data/%s' % celeb_id
    (conn, cur) = connectDB()
    for filename, url in urls.items():
        pathToFile = os.path.join(outputDir, 'images', filename)
        pathToS3 = os.path.join(outputS3, 'images', filename)
        if os.path.isfile(pathToFile):
            cur.execute(IMAGE_INSERT, (url, pathToFile, pathToS3, celeb_id))
            conn.commit()
    disconnectDB(conn, cur)
