import logging

import boto3
import glob
import os

from pathlib import Path

logger = logging.getLogger('uploader')


def upload(bundle, outdir, bucket, name, storage_class):
    logger.info('Uploading non-band files')

    s3 = boto3.resource('s3')

    meta_list = []
    for parent, dirs, files in os.walk(bundle):
        if parent == os.path.join(bundle, 'bands'):
            continue

        for f in files:
            relpath = os.path.relpath(os.path.join(parent, f), bundle)
            if relpath.startswith('.'):
                raise RuntimeError('Unexpected meta file: {}'.format(relpath))
            meta_list.append(relpath)

    for meta in meta_list:
        local = os.path.join(bundle, meta)
        remote = '{}/{}'.format(name, meta)

        logger.info('Uploading meta file %s -> %s', local, remote)

        with open(local, 'rb') as f:
            s3.Bucket(bucket).put_object(
                Key=remote, Body=f, StorageClass=storage_class)
