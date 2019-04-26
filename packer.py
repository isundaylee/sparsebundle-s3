#!/usr/bin/env python3

import sys
import os
import logging
import plistlib
import shutil
import tarfile

PACKAGE_COUNT = 0x10
STORAGE_CLASS = 'DEEP_ARCHIVE'

logger = logging.getLogger('packer')


def usage():
    print('Usage: packer.py bundle-file output-dir')
    exit(0)


def pack(bundle, outdir, package_count):
    # Read band size
    plist_path = os.path.join(bundle, 'Info.plist')
    if not os.path.exists(plist_path):
        logger.critical('Info.plist does not exist: %s', plist_path)
        exit(1)

    with open(plist_path, 'rb') as f:
        plist = plistlib.load(f)
    band_size = plist['band-size']
    logger.info('Band size: %d bytes', band_size)

    # Gather the list of bands
    bands_dir = os.path.join(bundle, 'bands')
    if not os.path.exists(bands_dir):
        logger.critical('Bundle bands directory does not exist: %s', bands_dir)
        exit(1)

    bands = []
    for f in os.listdir(bands_dir):
        try:
            num = int(f, 16)

            if f != format(num, 'x'):
                logger.critical('Invalid band file: %s', f)
                exit(1)
        except ValueError:
            logger.critical('Invalid band file: %s', f)
            exit(1)
        bands.append(num)
    bands = sorted(bands)
    logger.info('Band count: %d', len(bands))

    # Calculate packages
    packages = {}
    for band in bands:
        package_id = band // package_count
        if package_id not in packages:
            packages[package_id] = []
        packages[package_id].append(band)
    logger.info('Package count: %d', len(packages))

    # Do packing
    os.makedirs(outdir, exist_ok=True)
    for package_id in sorted(packages.keys()):
        start = format(package_id * package_count, 'x')
        end = format((package_id + 1) * package_count - 1, 'x')
        name = '{}-{}'.format(start, end)
        tmp_path = os.path.join(outdir, '{}-tmp.tar.gz'.format(name))
        path = os.path.join(outdir, '{}.tar.gz'.format(name))
        logger.info('Packing package %s', path)

        if os.path.exists(path):
            logger.info('  Already done')
            continue

        if os.path.exists(tmp_path):
            os.unlink(tmp_path)

        with tarfile.open(tmp_path, 'x:gz') as tar:
            bands = packages[package_id]
            for band in bands:
                band_name = format(band, 'x')
                band_path = os.path.join(bands_dir, band_name)
                logger.info('  Adding %s -> %s', band_name, band_path)
                tar.add(band_path, band_name)
        shutil.move(tmp_path, path)


def main():
    logging.basicConfig(level=logging.INFO)

    if len(sys.argv) < 3:
        usage()

    bundle = sys.argv[1]
    outdir = sys.argv[2]

    pack(bundle, outdir, PACKAGE_COUNT)


main()
