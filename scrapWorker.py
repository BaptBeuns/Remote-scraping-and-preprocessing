import os
import sys
from collections import OrderedDict
import shutil
import re
from celery import Celery
from kombu import Exchange, Queue
from django.db import connections
import subprocess

import scrapUtils
import preprocessUtils



sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'RCWD.settings_only_db')
import django
django.setup()
from celebrities.models import Celebrity, Images, Crop, Batch

app =  Celery('tasks', backend='amqp', broker='amqp://scrap_user:uzeraiyu@annotation.dev.azure.reminiz.com//')

CELERY_DEFAULT_QUEUE = 'default'
CELERY_QUEUES = (
                Queue('default', Exchange('default'), routing_key='default'),
                Queue('type1', Exchange('type1'), routing_key='type1'),
                Queue('type2', Exchange('type2'), routing_key='type2'),
            )

@app.task(ignore_result=True)
def scrapCeleb(celeb_id):
    # this scrip must be launched in a queue type1

    # scraping the images proposed by google image
    scrapUtils.prepareScraping(celeb_id)
    # crawl the google image path
    scrapUtils.crawlGoogle(celeb_id)
    # parse the web page
    scrapUtils.parsePage(celeb_id)
    # dl the files on a local repo
    scrapUtils.dlFiles(celeb_id)
    # upload everything on the s3
    scrapUtils.uploadScrap(celeb_id)
    # clean scrap
    scrapUtils.cleanScrap(celeb_id)
    # launch preprocessing
    preprocessCeleb.apply_async((celeb_id,), queue='type2')

@app.task(ignore_result=True)
def preprocessCeleb(celeb_id):
    # this scrip must be launch in a queue type2

    preprocessUtils.preparePreprocessing(celeb_id)
    # gets data from s3
    preprocessUtils.downloadScrap(celeb_id)
    # detects all faces and extracts the right crops
    preprocessUtils.detectFaces(celeb_id)
    # ranks the images and create the batches
    preprocessUtils.rankFaces(celeb_id)
    # upload everything to s3
    preprocessUtils.uploadPreprocessing(celeb_id)
    # clean preprocessing
    preprocessUtils.cleanPreprocessing(celeb_id)

@app.task(ignore_result=True)
def updateCelebStatus(celeb_id, (step, state)):
    celeb = Celebrity.objects.get(id=celeb_id)
    celeb.step = step
    celeb.state = state
    celeb.save()
    connections.close_all()
