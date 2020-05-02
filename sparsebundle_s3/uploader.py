import logging
import os
import hashlib
import base64

from pathlib import Path

import boto3
import botocore

import arc.archiver


def _calculate_md5(file):
    md5 = hashlib.md5()
    file.seek(0)
    for chunk in iter(lambda: file.read(1024 * 1024), b""):
        md5.update(chunk)
    return md5


class Uploader:
    def __init__(
        self,
        bundle,
        bundle_files,
        package_count,
        gzip,
        lz4,
        cache_chunks,
        outdir,
        bucket,
        name,
        storage_class,
        for_real,
    ):
        self.bundle = bundle
        self.bundle_files = bundle_files
        self.package_count = package_count
        self.gzip = gzip
        self.lz4 = lz4
        self.cache_chunks = cache_chunks
        self.outdir = outdir
        self.bucket = bucket
        self.name = name
        self.storage_class = storage_class
        self.for_real = for_real

        self.logger = logging.getLogger("uploader")

    def _upload_file(self, local_file, remote, md5_catalog_path, storage_class):
        md5 = _calculate_md5(local_file)

        try:
            e_tag = boto3.resource("s3").Object(self.bucket, remote).e_tag
            if e_tag[1:-1] == md5.hexdigest():
                self.logger.info("  File %s already uploaded.", remote)
                return
            else:
                self.logger.warning("  File %s has a checksum mismatch.", remote)
        except botocore.exceptions.ClientError:
            pass

        if not self.for_real:
            return

        self.logger.info("  Starting to write to %s", remote)

        try:
            local_file.seek(0)
            boto3.resource("s3").Bucket(self.bucket).put_object(
                Key=remote,
                Body=local_file,
                StorageClass=storage_class,
                ContentMD5=base64.b64encode(md5.digest()).decode(),
            )

            if md5_catalog_path is not None:
                with open(md5_catalog_path, "a") as file:
                    file.write("{} {}\n".format(md5.hexdigest(), remote))
        except botocore.exceptions.ClientError as ex:
            raise RuntimeError("Exception while uploading to S3: {}".format(ex))

    def _find_meta_files(self):
        meta_list = []
        for file in self.bundle_files:
            if Path(os.path.join(self.bundle, "bands")) in Path(file).parents:
                continue

            if os.path.isdir(file):
                continue

            relpath = os.path.relpath(file, self.bundle)
            if relpath.startswith("."):
                raise RuntimeError("Unexpected meta file: {}".format(relpath))
            meta_list.append(relpath)
        return meta_list

    def _find_bands(self):
        # Gather the list of bands
        bands_dir = os.path.join(self.bundle, "bands")
        if not os.path.exists(bands_dir):
            raise RuntimeError(
                "Bundle bands directory does not exist: {}".format(bands_dir)
            )

        bands_path = Path(bands_dir)
        bands = []
        for f in self.bundle_files:
            # Skips the bands/ folder itself
            if Path(f) == bands_path:
                continue

            # Skips files that are not under bands/
            if bands_path not in Path(f).parents:
                continue

            base = os.path.basename(f)
            try:
                num = int(base, 16)

                if base != format(num, "x"):
                    raise RuntimeError("Invalid band file: {}".format(f))
            except ValueError:
                raise RuntimeError("Invalid band file: {}".format(f))
            bands.append(num)
        bands = sorted(bands)

        return bands

    def _build_package_manifests(self, bands):
        packages = {}
        for band in bands:
            package_id = band // self.package_count
            if package_id not in packages:
                packages[package_id] = []
            packages[package_id].append(band)
        return packages

    def upload(self):
        md5_catalog_path = os.path.join(self.outdir, "checksums.txt")

        self.logger.info("Uploading meta files")
        for meta in self._find_meta_files():
            local = os.path.join(self.bundle, meta)
            remote = "{}/{}".format(self.name, meta)

            self.logger.info("Uploading meta file %s -> %s", local, remote)
            with open(local, "rb") as file:
                self._upload_file(file, remote, md5_catalog_path, self.storage_class)

        bands = self._find_bands()
        packages = self._build_package_manifests(bands)
        self.logger.info(
            "Found %d bands -- will build %d packages", len(bands), len(packages)
        )

        for package_id in sorted(packages.keys()):
            name = "{}-{}".format(
                format(package_id * self.package_count, "x"),
                format((package_id + 1) * self.package_count - 1, "x"),
            )
            remote_path = "{}/bands/{}.arc".format(self.name, name)

            self.logger.info("Archiving package %s", remote_path)
            archive = arc.archiver.Archiver(
                use_gzip=self.gzip, use_lz4=self.lz4, cache_chunks=self.cache_chunks
            )
            band_files = []
            for band in packages[package_id]:
                band_name = format(band, "x")
                band_path = os.path.join(self.bundle, "bands", band_name)
                band_file = open(band_path, "rb")
                band_files.append(band_file)
                archive.add_file(band_name, band_file)

            self.logger.info("  Uploading package %s", remote_path)
            self._upload_file(
                archive, remote_path, md5_catalog_path, self.storage_class
            )

            for file in band_files:
                file.close()

        local = os.path.join(md5_catalog_path)
        remote = "{}/checksums.txt".format(self.name)
        self.logger.info("Uploading checksum file %s -> %s", local, remote)
        with open(local, "rb") as file:
            self._upload_file(file, remote, None, "STANDARD")
