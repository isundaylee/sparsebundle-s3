import struct
import os


def _get_length(content):
    if hasattr(content, 'read'):
        return os.stat(content.name).st_size
    else:
        return len(content)


class Archive:
    """
    Archive binary format is composed of a stream of files. Each file has the
    following fields:

    1. name_len,    4           bytes (little endian)
    2. name,        name_length bytes
    1. content_len, 8           bytes (little endian)
    2. content,     content_len bytes
    """

    MAGIC = b'arcf'

    HEADER_LEN = 32

    def __init__(self):
        self.fields = []
        self._add_field(Archive.MAGIC)
        self._add_field(b'\x00' * Archive.HEADER_LEN)

        self.field_idx = 0
        self.field_pos = 0

    def add_file(self, name, content):
        """Adds the given file into the archive.

        `name` should be a string.
        `content` should be a bytes, a bytearray, or an opened file-like
        object."""
        self._add_field(struct.pack("<L", len(name)))
        self._add_field(name.encode())
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
