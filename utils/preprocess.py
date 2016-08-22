#!/usr/bin/env python

# --------------------------------------------------------
# Faster R-CNN
# Copyright (c) 2015 Microsoft
# Licensed under The MIT License [see LICENSE for details]
# Written by Ross Girshick
# --------------------------------------------------------

"""
Demo script showing detections in sample images.

See README.md for installation instructions before running.
"""

import _init_paths
from fast_rcnn.config import cfg
from fast_rcnn.test import im_detect
from fast_rcnn.nms_wrapper import nms
import caffe, cv2

caffe.set_mode_gpu()
caffe.set_device(0)

import sys, os, shutil

import numpy as np
from scipy.spatial.distance import cdist
from tqdm import tqdm
import scipy.io as sio

import subprocess

import argparse
from PIL import Image
import uuid


MOUNTDATA = "/disk2/tonio/scrap_type_2/"
INSERT_IMAGE = "INSERT INTO images (url, local_path, s3_path, celebrity_id) VALUES (%s, %s, %s, %s)"
INSERT_CROP = "INSERT INTO crop (local_path, s3_path, x1, y1, x2, y2, score, batch_id, celebrity_id, image_id) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
INSERT_BATCH = "INSERT INTO batch (rank, annotated, celebrity_id) VALUES (%s, %s, %s)"
UPDATE_CELEB = "UPDATE celebrity SET step=%s, state=%s WHERE celebrity.id = %s"
UPDATE_CROP = "UPDATE crop SET batch_id=%s, score=%s WHERE crop.id = %s"
LIST_CROPS = "SELECT * FROM crop WHERE celebrity_id = %s;"
LIST_IMAGES = "SELECT * FROM images WHERE celebrity_id = %s;"


def updateCelebStatus(celeb_id, (step, state), connection, cursor):
    # updates celeb status
    cursor.execute(UPDATE_CELEB, (step, state, celeb_id))
    connection.commit()

def preparePreprocessing(celeb_id):
    outputDir = '/disk2/data_scrap_type2/%s' % celeb_id
    if os.path.isdir(outputDir):
        shutil.rmtree(outputDir)
    os.mkdir(outputDir)
    os.mkdir(os.path.join(outputDir, 'crops'))
    return

def cleanPreprocessing(celeb_id):
    outputDir = '/disk2/data_scrap_type2/%s' % celeb_id
    if os.path.isdir(outputDir):
        shutil.rmtree(outputDir)
    return

def downloadScrap(celeb_id):
    pathToS3 = 's3://rcwd/data/%s/' % celeb_id
    outputDir = '/disk2/data_scrap_type2/%s/' % celeb_id
    cmd = ['s3cmd', 'get', '--verbose', '--recursive', pathToS3, outputDir]
    p = subprocess.Popen(cmd)
    out, err = p.communicate()

    imageDir = "%simages" % (outputDir)
    dirHasImages = os.path.isdir(imageDir) and bool(os.listdir(imageDir))
    return dirHasImages

def uploadPreprocessing(celeb_id):
    pathToS3 = 's3://rcwd/data/%s/crops/' % celeb_id
    outputDir = '/disk2/data_scrap_type2/%s/crops/' % celeb_id
    cmd = ['s3cmd', 'put', '--verbose', '--recursive', outputDir, pathToS3]
    p = subprocess.Popen(cmd)
    out, err = p.communicate()
    return

def initDetection():
    cfg.TEST.HAS_RPN = True  # Use RPN for proposals

    prototxt = '/data/models/faceDetector/zf_models/zf_rcnn_test.pt'
    caffemodel = '/data/models/faceDetector/zf_models/zf_rcnn_final_new.caffemodel'

    print prototxt
    print caffemodel
    net = caffe.Net(prototxt, caffemodel, caffe.TEST)
    return net

def detect(net, im):
    """Detect object classes in an image using pre-computed object proposals."""

    # Detect all object classes and regress object bounds
    # print im.shape
    # print max(im.shape[:2])
    cfg.TEST.SCALES = (max(im.shape[:2]),)
    cfg.TEST.MAX_SIZE = 2000
    scores, boxes = im_detect(net, im)

    # Visualize detections for each class
    CONF_THRESH = 0.8
    NMS_THRESH = 0.3
    cls_boxes = boxes[:, 4:]
    cls_scores = scores[:, 1]
    dets = np.hstack((cls_boxes,
                      cls_scores[:, np.newaxis])).astype(np.float32)
    # print dets
    keep = nms(dets, NMS_THRESH)
    dets = dets[keep, :]
    ind = np.where(dets[:, 4] > 0.5)[0]
    dets = dets[ind, :]

    return dets

def rescaleBox((x, y, w, h), s):
    return (x + 0.5 * w * (1. - 1. / s),
            y + 0.5 * h * (1. - 1. / s),
            w / s,
            h / s)

def recenterBox((x, y, w, h)):
    centerX = x + 0.5*w
    centerY = y + 0.5*h
    w = (w + h) * 0.5
    return (centerX - 0.5*w,
            centerY - 0.5*w,
            w,
            w)

def detectFaces(celeb_id, connection, cursor, logger):
    # detects all the faces from the images in /home/reminiz/preprocessing/mnt/data/celeb_id/images
    # saves the crops into /home/reminiz/preprocessing/mnt/data/celeb_id/crops

    outputDir = '/disk2/data_scrap_type2/%s' % celeb_id

    updateCelebStatus(celeb_id, (3, 1), connection, cursor)

    logger.info("Detection network initialisation")
    net = initDetection()
    logger.info("Detection network initialisation OK")
    pathToCrops = os.path.join(outputDir, 'crops')
    pathToS3 = 'rcwd.s3.amazonaws.com/data/%s/crops' % celeb_id

    # celeb = Celebrity.objects.get(id=celeb_id)
    # images = Images.objects.filter(celebrity=celeb)
    logger.info("Get all images")
    cursor.execute(LIST_IMAGES, (celeb_id,))
    images = cursor.fetchall()
    logger.info("Get all images OK")
    crop_count = 0

    logger.info("Image detection loop for %s images" % len(images))
    for image in tqdm(images):
        path_to_file, filename = os.path.split(image['local_path'])
        fileName = '/disk2/data_scrap_type2/%s/images/%s' % (celeb_id, filename)
        print fileName
        im = cv2.imread(fileName)
        if im is None:
            continue
        if (im.shape[0] < 100 or im.shape[1] < 100):
            continue
        dets = detect(net, im)
        if dets.shape[0] > 0:
            im = Image.open(fileName)
            for det in dets:
                # det is x1, y1, x2, y2 format
                # face is x, y, w, h format
                face = [det[0], det[1], det[2]-det[0], det[3]-det[1]]
                # file to save the crop
                crop_name = uuid.uuid4().hex
                pathCrop = os.path.join(pathToCrops, '%s.jpg') % crop_name
                pathS3 = os.path.join(pathToS3, '%s.jpg') % crop_name
                # rescaling the box
                face = recenterBox(face)
                face = rescaleBox(face, 0.9)
                # croping the image
                crop = im.crop([int(c) for c in
                                [face[0], face[1], face[0]+face[2], face[1]+face[3]]])
                crop = crop.resize((224, 224), Image.ANTIALIAS)
                crop = crop.convert('RGB')
                crop.save(pathCrop, quality=95)
                # saving the crop in the db
                cursor.execute(INSERT_CROP, (pathCrop, pathS3, face[0], face[1],
                    face[0]+face[2], face[1]+face[3], 0, None, celeb_id, image['id']))
                crop_count += 1

        updateCelebStatus(celeb_id, (3, 2), connection, cursor)

        updateCelebStatus(celeb_id, (4, 0), connection, cursor)
    logger.info("Image detection loop for %s images OK" % len(images))

def initEmbedding():

    prototxt = '/data/models/CNN_ZOO/vgg_face_caffe/VGG_FACE_deploy.prototxt'
    caffemodel = '/data/models/CNN_ZOO/vgg_face_caffe/VGG_FACE.caffemodel'

    net = caffe.Net(prototxt, caffemodel, caffe.TEST)

    means = [93.1863, 104.7624, 129.1863]

    return net, means

def prepareBlob(faces):
    # create a blob of shape (len(faces), 3, 224, 224)
    # from an iterator of Crop objects
    blob = np.zeros((len(faces), 224, 224, 3))
    for count, face in enumerate(faces):
        im = cv2.imread(face['local_path'])
        blob[count, ...] = im.copy()
    return blob.transpose([0, 3, 1, 2]).copy()

def normalizeBlob(blob, means):
    # create a blob of shape (len(faces), 3, 224, 224)
    # from an iterator of Crop objects
    blob[:, 0, ...] -= means[0]
    blob[:, 1, ...] -= means[1]
    blob[:, 2, ...] -= means[2]
    return blob

def rankFaces(celeb_id, connection, cursor, logger):

    outputDir = '/disk2/data_scrap_type2/%s' % celeb_id
    if not os.path.isdir(outputDir):
        print 'Folder %s does not exists' % outputDir
        # connections.close_all()
        sys.exit()

    updateCelebStatus(celeb_id, (4, 1), connection, cursor)
    logger.info("Embedding initialisation")
    net, means = initEmbedding()
    logger.info("Embedding initialisation OK")

    pathToCrops = os.path.join(outputDir, 'crops')

    cursor.execute(LIST_CROPS, (celeb_id,))
    crops = cursor.fetchall()
    logger.info("Retrieved %s crops of celeb_id %s" % (len(crops), celeb_id))

    blob = prepareBlob(crops)
    print 'blob shape :', blob.shape
    blob = normalizeBlob(blob, means)
    # connections.close_all()

    outs = np.zeros((blob.shape[0], 4096))
    batch_size = 64
    for i in tqdm(range(0, blob.shape[0], batch_size)):
        blob_temp = blob[i:i+batch_size, ...].copy()
        net.blobs['data'].reshape(*(blob_temp.shape))
        outs[i:i+batch_size, ...] = net.forward(data=blob_temp, blobs=['fc7'])['fc7'].copy()


    logger.info('computing distances')
    outs /= np.linalg.norm(outs, axis=1)[..., np.newaxis]
    n_ref_images = 10
    batch_size = 50
    referenceFaces = outs[:n_ref_images]
    toCompareFaces = outs[n_ref_images:]
    scores = cdist(referenceFaces, toCompareFaces).mean(axis=0)
    inds = np.argsort(scores)

    # celeb = Celebrity.objects.get(id=celeb_id)
    # crops = Crop.objects.filter(celebrity=celeb)
    cursor.execute(LIST_CROPS, (celeb_id,))
    crops = cursor.fetchall()


    logger.info('inserting data')
    crops_to_insert = [crops[n_ref_images + i] for i in inds]
    scores_to_insert = [scores[i] for i in inds]
    for count, crop in tqdm(enumerate(crops_to_insert)):
        if count % batch_size == 0:
            batch_current = count / batch_size
            cursor.execute(INSERT_BATCH, (batch_current, False, celeb_id,))
            batch_id = cursor.lastrowid
            # batch = Batch.objects.create(celebrity=celeb, rank=batch_current, annotated=False)
        # print count, batch_size
        # print batch_current, batch_id
        cursor.execute(UPDATE_CROP, (batch_id, scores_to_insert[count], crop['id'],))
        connection.commit()
        # crop.score = scores_to_insert[count]
        # crop.batch = batch
        # crop.save()

    updateCelebStatus(celeb_id, (4, 2), connection, cursor)

    updateCelebStatus(celeb_id, (5, 0), connection, cursor)
