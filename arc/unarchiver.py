import struct
import gzip

import lz4.frame

from .common import *


class FileWrapper:
    def __init__(self, file, offset, length, flags):
        self.file = file
        self.offset = offset
        self.length = length
        self.flags = flags

        self.pos = 0

    def _compute_cache(self):
        self.file.seek(self.offset)
        compressed = self.file.read(self.length)

        if self.flags & FLAG_GZIP != 0:
            self.decompressed = gzip.decompress(compressed)
        elif self.flags & FLAG_LZ4 != 0:
            self.decompressed = lz4.frame.decompress(compressed)
        else:
            assert False

    def _clear_cache(self):
        self.decompressed = None

    def read(self, size):

        if (self.flags & FLAG_GZIP != 0) or (self.flags & FLAG_LZ4 != 0):
            self._compute_cache()
            to_read = min(size, len(self.decompressed) - self.pos)
            result = self.decompressed[self.pos : self.pos + to_read]
            self.pos += to_read
            if self.pos == self.length:
                self._clear_cache()
            return result
        else:
            to_read = min(size, self.length - self.pos)
            self.file.seek(self.offset + self.pos)

            self.pos += to_read

            return self.file.read(to_read)

    def seek(self, pos):
        self.pos = pos


class Unarchiver:
    def __init__(self, file):
        self.file = file

    def files(self):
        results = []

        self.file.seek(0)

        if self.file.read(4) != MAGIC:
            raise RuntimeError("Invalid magic bytes.")

        flags = struct.unpack("<L", self.file.read(4))[0]

        if self.file.read(28) != b"\x00" * 28:
            raise RuntimeError("Invalid header padding bytes.")

        while True:
            name_len_bytes = self.file.read(4)

            if name_len_bytes == b"":
                return results

            name_len = struct.unpack("<L", name_len_bytes)[0]
            name = self.file.read(name_len).decode()

            content_len = struct.unpack("<Q", self.file.read(8))[0]

            results.append(
                (name, FileWrapper(self.file, self.file.tell(), content_len, flags))
            )
            self.file.seek(content_len, 1)
