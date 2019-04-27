import logging

import boto3
import glob
import os

from pathlib import Path

logger = logging.getLogger('uploader')


def upload_file(local, bucket, remote, storage_class):
    with open(local, 'rb') as f:
        boto3.resource('s3').Bucket(bucket).put_object(
            Key=remote, Body=f, StorageClass=storage_class)


def upload(bundle, outdir, bucket, name, storage_class, package_queue):
    logger.info('Uploading non-band files')

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
        upload_file(local, bucket, remote, storage_class)

    while True:
        package = package_queue.get()
        if package is None:
            break

        local = os.path.join(outdir, '{}.tar.gz'.format(package))
        remote = '{}/bands/{}.tar.gz'.format(name, package)

        logger.info('Uploading band file %s -> %s', local, remote)
        upload_file(local, bucket, remote, storage_class)
