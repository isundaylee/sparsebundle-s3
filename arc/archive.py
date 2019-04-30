import struct


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
        self.files = []
        self.fields = [Archive.MAGIC, b'\x00' * Archive.HEADER_LEN]

        self.field_idx = 0
        self.field_pos = 0

    def add_file(self, name, content):
        """Adds the given file into the archive.

        `name` should be a string.
        `content` should be a bytes or a bytearray."""
        self.files.append((name, content))

        self.fields.append(struct.pack("<L", len(name)))
        self.fields.append(name.encode())
        self.fields.append(struct.pack("<Q", len(content)))
        self.fields.append(content)

    def __len__(self):
        result = len(Archive.MAGIC) + Archive.HEADER_LEN

        for name, content in self.files:
            result += 4
            result += len(name)
            result += 8
            result += len(content)

        return result

    def read(self, size):
        if self.field_idx >= len(self.fields):
            return b''

        cur_field_remaining = len(self.fields[self.field_idx]) - self.field_pos
        to_read = min(cur_field_remaining, size)
        result = self.fields[self.field_idx][self.field_pos:self.field_pos + to_read]

        self.field_pos += to_read
        if self.field_pos == len(self.fields[self.field_idx]):
            self.field_idx += 1
            self.field_pos = 0

        return result
