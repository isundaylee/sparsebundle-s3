import logging
import os
import touch
import glob
import time

import boto3
import botocore

from pathlib import Path

logger = logging.getLogger('uploader')


def upload_file(local, bucket, remote, storage_class):
    with open(local, 'rb') as f:
        boto3.resource('s3').Bucket(bucket).put_object(
            Key=remote, Body=f, StorageClass=storage_class)


def upload(bundle, bundle_files, outdir, bucket, name, storage_class,
           package_queue, stop_event):
    logger.info('Finding non-band files')
    meta_list = []
    for f in bundle_files:
        if Path(os.path.join(bundle, 'bands')) in Path(f).parents:
            continue

        if os.path.isdir(f):
            continue

        relpath = os.path.relpath(f, bundle)
        if relpath.startswith('.'):
            raise RuntimeError('Unexpected meta file: {}'.format(relpath))
        meta_list.append(relpath)

    logger.info('Uploading non-band files')
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
        local_done = os.path.join(outdir, '{}.done'.format(package))
        remote = '{}/bands/{}.tar.gz'.format(name, package)

        if os.path.exists(local_done):
            logger.info('Already uploaded band file %s', local)
            continue

        logger.info('Uploading band file %s -> %s', local, remote)
        upload_file(local, bucket, remote, storage_class)
        os.unlink(local)
        touch.touch(local_done)

        if stop_event.is_set():
            logger.info('Stopping...')
            break
