#!/usr/bin/env python2.7

import argparse
import os
import sys
import MySQLdb
import subprocess
import re
from lxml import html
from django.db import connections

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'RCWD.settings_only_db')
import django
django.setup()
from celebrities.models import Celebrity
def parseGoogleWebPage(pageFile):

    filesToDownload = []
    with open(pageFile) as file_id:
        page = file_id.read().strip('\n')
        tree = html.fromstring(page)
        images = tree.xpath('//div[@class="rg_di rg_el ivg-i"]')
        for count, image in enumerate(images):
            try:
                a = image.getchildren()[0]
                href = a.attrib['href']
                m = re.search("(?<=imgurl=)(.*?)(?=&imgrefurl)", href)
                imageUrl = m.group(0)
                filesToDownload.append(imageUrl)
            except:
                print 'not existing image'
            if count > 10:
                break
    return filesToDownload

def writeUrls(filesToDownload, pageDst):
    with open(pageDst, 'w') as file_id:
        for count, imageUrl in enumerate(filesToDownload):
            file_id.write('%s\n\tout=%s\n' % (imageUrl, count))

if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument('celeb_id', type=int)

    args = parser.parse_args()

    try:
        celeb = Celebrity.objects.get(id=args.celeb_id)
    except:
        print 'Celeb %s does not exist in DB' % args.celeb_id

    print celeb.name, celeb.searchQuery

    if not (celeb.step, celeb.state) == (1, 0):
         print 'Celeb %s (%s, %s) is not in the right state' % (args.celeb_id,
                                                                 celeb.step, celeb.state)
         sys.exit()
    outputDir = '/mnt/data/%s' % args.celeb_id

    if not os.path.isdir(outputDir):
        print 'Folder %s does not exists' % outputDir
        sys.exit()

    celeb.state = 1
    celeb.save()
    connections.close_all() # to avoid timout

    pageFile = os.path.join(outputDir, 'page.html')
    pageDst = os.path.join(outputDir, 'list_files.txt')

    filesToDownload = parseGoogleWebPage(pageFile)

    writeUrls(filesToDownload, pageDst)

    celeb = Celebrity.objects.get(id=args.celeb_id)
    celeb.state = 2
    celeb.save()
    connections.close_all() # to avoid timout

    celeb = Celebrity.objects.get(id=args.celeb_id)
    celeb.step = 2
    celeb.state = 0
    celeb.save()
    connections.close_all() # to avoid timout
