import struct

from common import *


class FileWrapper:
    def __init__(self, file, offset, length, flags):
        self.file = file
        self.offset = offset
        self.length = length
        self.flags = flags

        self.pos = 0

    def read(self, size):
        self.file.seek(self.offset + self.pos)

        to_read = min(size, self.length - self.pos)
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

        flags = struct.unpack('<L', self.file.read(4))[0]

        if self.file.read(28) != b'\x00' * 28:
            raise RuntimeError("Invalid header padding bytes.")

        while True:
            name_len_bytes = self.file.read(4)

            if name_len_bytes == b'':
                return results

            name_len = struct.unpack('<L', name_len_bytes)[0]
            name = self.file.read(name_len).decode()

            content_len = struct.unpack('<Q', self.file.read(8))[0]

            results.append((
                name,
                FileWrapper(self.file, self.file.tell(),
                            content_len, flags)
            ))
            self.file.seek(content_len, 1)
