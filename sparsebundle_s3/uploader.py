import logging
import os
import touch
import hashlib
import base64

import boto3
import botocore

from pathlib import Path


class Uploader:
    def __init__(self, bundle, bundle_files, outdir, bucket, name,
                 storage_class, package_queue, stop_event):
        self.bundle = bundle
        self.bundle_files = bundle_files
        self.outdir = outdir
        self.bucket = bucket
        self.name = name
        self.storage_class = storage_class

        self.package_queue = package_queue
        self.stop_event = stop_event

        self.logger = logging.getLogger('uploader')

    def _upload_file(self, local, remote, md5_catalog_path):
        m = hashlib.md5()
        with open(local, 'rb') as f:
            for chunk in iter(lambda: f.read(1024 * 1024), b""):
                m.update(chunk)

        if md5_catalog_path is not None:
            with open(md5_catalog_path, 'a') as f:
                f.write("{} {}\n".format(m.hexdigest(), remote))

        try:
            with open(local, 'rb') as f:
                boto3.resource('s3').Bucket(self.bucket).put_object(
                    Key=remote, Body=f, StorageClass=self.storage_class,
                    ContentMD5=base64.b64encode(m.digest()).decode())
        except botocore.exceptions.ClientError as e:
            raise RuntimeError("Exception while uploading to S3: {}".format(e))

    def upload(self):
        self.logger.info('Finding non-band files')
        meta_list = []
        for f in self.bundle_files:
            if Path(os.path.join(self.bundle, 'bands')) in Path(f).parents:
                continue

            if os.path.isdir(f):
                continue

            relpath = os.path.relpath(f, self.bundle)
            if relpath.startswith('.'):
                raise RuntimeError('Unexpected meta file: {}'.format(relpath))
            meta_list.append(relpath)

        md5_catalog_path = os.path.join(self.outdir, "checksums.txt")

        self.logger.info('Uploading non-band files')
        for meta in meta_list:
            local = os.path.join(self.bundle, meta)
            remote = '{}/{}'.format(self.name, meta)

            self.logger.info('Uploading meta file %s -> %s', local, remote)
            self._upload_file(local, remote, md5_catalog_path)

        while True:
            package = self.package_queue.get()
            if package is None:
                break

            local = os.path.join(self.outdir, '{}.tar.gz'.format(package))
            local_done = os.path.join(self.outdir, '{}.done'.format(package))
            remote = '{}/bands/{}.tar.gz'.format(self.name, package)

            if os.path.exists(local_done):
                self.logger.info('Already uploaded band file %s', local)
                continue

            self.logger.info('Uploading band file %s -> %s', local, remote)
            self._upload_file(local, remote, md5_catalog_path)
            os.unlink(local)
            touch.touch(local_done)

            if self.stop_event.is_set():
                self.logger.info('Stopping...')
                return

        local = os.path.join(md5_catalog_path)
        remote = '{}/checksums.txt'.format(self.name)
        self.logger.info('Uploading checksum file %s -> %s', local, remote)
        self._upload_file(local, remote, None)
