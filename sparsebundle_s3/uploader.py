import logging
import os
import hashlib
import base64

from pathlib import Path

import touch
import boto3
import botocore


def _calculate_md5(path):
    md5 = hashlib.md5()
    with open(path, 'rb') as file:
        for chunk in iter(lambda: file.read(1024 * 1024), b""):
            md5.update(chunk)
    return md5


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
        md5 = _calculate_md5(local)

        if md5_catalog_path is not None:
            with open(md5_catalog_path, 'a') as file:
                file.write("{} {}\n".format(md5.hexdigest(), remote))

        try:
            with open(local, 'rb') as file:
                boto3.resource('s3').Bucket(self.bucket).put_object(
                    Key=remote, Body=file, StorageClass=self.storage_class,
                    ContentMD5=base64.b64encode(md5.digest()).decode())
        except botocore.exceptions.ClientError as ex:
            raise RuntimeError(
                "Exception while uploading to S3: {}".format(ex))

    def _find_meta_files(self):
        meta_list = []
        for file in self.bundle_files:
            if Path(os.path.join(self.bundle, 'bands')) in Path(file).parents:
                continue

            if os.path.isdir(file):
                continue

            relpath = os.path.relpath(file, self.bundle)
            if relpath.startswith('.'):
                raise RuntimeError('Unexpected meta file: {}'.format(relpath))
            meta_list.append(relpath)
        return meta_list

    def upload(self):
        md5_catalog_path = os.path.join(self.outdir, "checksums.txt")

        self.logger.info('Uploading meta files')
        for meta in self._find_meta_files():
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
