import struct
import os
import gzip
import io

import lz4.frame
import hexdump


def _get_length(content):
    if hasattr(content, '__len__'):
        return len(content)
    elif hasattr(content, 'read'):
        return os.stat(content.name).st_size
    else:
        raise NotImplementedError()


class TransformWrapper:
    def __init__(self, data):
        self.data = data
        self.compressed = None
        self.pos = 0

    def _transform(self, data):
        raise NotImplementedError()

    def _compute_cache(self):
        if self.compressed is not None:
            return

        self.compressed = self._transform(self.data)

    def _clear_cache(self):
        self.compressed = None

    def __len__(self):
        self._compute_cache()
        result = len(self.compressed)
        self._clear_cache()
        return result

    def seek(self, pos):
        self.pos = pos

    def read(self, size):
        self._compute_cache()

        to_read = min(size, len(self.compressed) - self.pos)
        result = self.compressed[self.pos:self.pos+to_read]
        self.pos += to_read

        if self.pos == len(self.compressed):
            self._clear_cache()

        return result


class GzipWrapper(TransformWrapper):
    def _transform(self, data):
        buf = io.BytesIO()
        with gzip.GzipFile(fileobj=buf, mode='wb', compresslevel=9, mtime=0) as gz:
            if hasattr(data, 'read'):
                data.seek(0)
                for chunk in iter(lambda: data.read(1024 * 1024), b''):
                    gz.write(chunk)
            else:
                gz.write(data)
        return buf.getvalue()


class Lz4Wrapper(TransformWrapper):
    def _transform(self, data):
        if hasattr(data, 'read'):
            data.seek(0)
            content = data.read()
        else:
            content = data

        compressed = lz4.frame.compress(content, compression_level=1,
                                        store_size=False, content_checksum=True)
        return compressed


class Archive:
    """
    arc binary format is composed of a header followed by a stream of files.

    The header contains the following fields:

    1. magic,       4           bytes (always "arcf")
    2. flags        4           bytes
        FLAG_GZIP   0x01        If set, all `content` fields will be gzipped
                                with compression level 9 and mtime fixed to 0.
                                `content_len` will be adjusted accordingly.
        FLAG_LZ4    0x02        If set, all `content` fields will be lz4 zipped
                                with compression level 1. `content_len` will be
                                adjusted accordingly.
    3. header_pad,  28          bytes (all 0 bits)

    Each file contains the following fields:

    1. name_len,    4           bytes (little endian)
    2. name,        name_length bytes
    1. content_len, 8           bytes (little endian)
    2. content,     content_len bytes
    """

    MAGIC = b'arcf'

    HEADER_LEN = 32

    FLAG_GZIP = 0x01
    FLAG_LZ4 = 0x02

    HEADER_PADDING_LEN = 28

    def __init__(self, gzip=False, lz4=False):
        self.fields = []
        self._add_field(Archive.MAGIC)

        self.flags = 0

        if gzip:
            self.flags |= Archive.FLAG_GZIP
        elif lz4:
            self.flags |= Archive.FLAG_LZ4

        self._add_field(struct.pack('<L', self.flags))
        self._add_field(b'\x00' * Archive.HEADER_PADDING_LEN)

        self.field_idx = 0
        self.field_pos = 0

    def add_file(self, name, content):
        """Adds the given file into the archive.

        `name` should be a string.
        `content` should be a bytes, a bytearray, or an opened file-like
        object."""
        self._add_field(struct.pack("<L", len(name)))
        self._add_field(name.encode())

        if self.flags & Archive.FLAG_GZIP != 0:
            content = GzipWrapper(content)
        elif self.flags & Archive.FLAG_LZ4 != 0:
            content = Lz4Wrapper(content)

        self._add_field(struct.pack("<Q", _get_length(content)))
        self._add_field(content)

    def _add_field(self, content):
        self.fields.append((_get_length(content), content))

    def __len__(self):
        return sum(map(lambda f: f[0], self.fields))

    def read(self, size):
        if self.field_idx >= len(self.fields):
            return b''

        field_len, field_content = self.fields[self.field_idx]

        cur_field_remaining = field_len - self.field_pos
        to_read = min(cur_field_remaining, size)

        if hasattr(field_content, 'read'):
            field_content.seek(self.field_pos)
            result = field_content.read(to_read)
            assert(len(result) == to_read)
        else:
            result = field_content[self.field_pos:self.field_pos + to_read]

        self.field_pos += to_read
        if self.field_pos == field_len:
            self.field_idx += 1
            self.field_pos = 0

        return result

    def seek(self, pos):
        self.field_idx = 0
        self.field_pos = pos

        while self.field_pos >= self.fields[self.field_idx][0]:
            self.field_pos -= self.fields[self.field_idx][0]
            self.field_idx += 1

            if self.field_idx >= len(self.fields):
                self.field_pos = 0
                return
