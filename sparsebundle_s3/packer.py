import sys
import os
import logging
import plistlib
import shutil
import tarfile
import glob
import time

from pathlib import Path

BACK_PRESSURE_LIMIT = 3
BACK_PRESSURE_SLEEP_INTERVAL = 1


class Packer:
    def __init__(self, bundle, bundle_files, outdir, package_count, package_queue, stop_event):
        self.bundle = bundle
        self.bundle_files = bundle_files
        self.outdir = outdir
        self.package_count = package_count

        self.package_queue = package_queue
        self.stop_event = stop_event

        self.logger = logging.getLogger('packer')

    def _find_bands(self):
        # Gather the list of bands
        bands_dir = os.path.join(self.bundle, 'bands')
        if not os.path.exists(bands_dir):
            raise RuntimeError(
                'Bundle bands directory does not exist: {}'.format(bands_dir))

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

                if base != format(num, 'x'):
                    raise RuntimeError('Invalid band file: {}'.format(f))
            except ValueError:
                raise RuntimeError('Invalid band file: {}'.format(f))
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

    def _build_package(self, bands, outfile):
        with tarfile.open(outfile, 'x:gz') as tar:
            for band in bands:
                band_name = format(band, 'x')
                band_path = os.path.join(self.bundle, 'bands', band_name)
                tar.add(band_path, band_name)

    def _back_pressure_wait(self):
        while True:
            outstanding_count = len(
                glob.glob(os.path.join(self.outdir, '*.tar.gz')))
            if outstanding_count < BACK_PRESSURE_LIMIT:
                break
            time.sleep(BACK_PRESSURE_SLEEP_INTERVAL)

    def pack(self):
        # Read band size
        plist_path = os.path.join(self.bundle, 'Info.plist')
        if not os.path.exists(plist_path):
            raise RuntimeError(
                'Info.plist does not exist: {}'.format(plist_path))

        with open(plist_path, 'rb') as f:
            plist = plistlib.load(f)
        band_size = plist['band-size']
        self.logger.info('Band size: %d bytes', band_size)

        # Calculate package manifests
        bands = self._find_bands()
        self.logger.info('Band count: %d', len(bands))
        packages = self._build_package_manifests(bands)
        self.logger.info('Package count: %d', len(packages))

        # Do packing
        os.makedirs(self.outdir, exist_ok=True)
        for package_id in sorted(packages.keys()):
            self._back_pressure_wait()

            name = '{}-{}'.format(
                format(package_id * self.package_count, 'x'),
                format((package_id + 1) * self.package_count - 1, 'x'))
            tmp_path = os.path.join(self.outdir, '{}-tmp.tar.gz'.format(name))
            done_path = os.path.join(self.outdir, '{}.done'.format(name))
            path = os.path.join(self.outdir, '{}.tar.gz'.format(name))

            self.logger.info('Packing package %s', path)

            # If the package is already built or uploaded
            if os.path.exists(path) or os.path.exists(done_path):
                self.logger.info('  Already done')
                self.package_queue.put(name)
                continue

            # Removes previous in-progress file
            if os.path.exists(tmp_path):
                os.unlink(tmp_path)

            self._build_package(packages[package_id], tmp_path)
            shutil.move(tmp_path, path)

            self.package_queue.put(name)

            if self.stop_event.is_set():
                self.logger.info('Stopping...')
                break

        self.package_queue.put(None)
