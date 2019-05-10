import unittest
import tempfile

from arc.archiver import Archiver


class UnseekableFile:
    def __init__(self, content):
        self.content = content
        self.pos = 0

    def read(self, size=1e10):
        to_read = min(len(self.content) - self.pos, size)

        result = self.content[self.pos:self.pos+to_read]
        self.pos += to_read

        return result

    def seek(self, new_pos):
        if new_pos == self.pos:
            return

        raise NotImplementedError("UnseekableFile cannot be seeked.")


def read_all(file, chunk_size=8192):
    content = b''
    while True:
        chunk = file.read(chunk_size)
        if len(chunk) == 0:
            break
        content += chunk
    return content


class TestArchiver(unittest.TestCase):
    def test_empty(self):
        arc = Archiver()

        expected = b'arcf' + b'\x00' * 32

        self.assertEqual(len(arc), len(expected))
        self.assertEqual(read_all(arc), expected)

    def test_one_file(self):
        arc = Archiver()

        arc.add_file("test", b"testcontent")

        expected = \
            b'arcf' + \
            b'\x00' * 32 + \
            b'\x04\x00\x00\x00' + \
            b'test' + \
            b'\x0b\x00\x00\x00\x00\x00\x00\x00' + \
            b'testcontent'

        self.assertEqual(len(arc), len(expected))
        self.assertEqual(read_all(arc), expected)

    def test_multiple_files(self):
        arc = Archiver()

        arc.add_file("test", b"testcontent")
        arc.add_file("wow", b"suchgreatstuff")

        expected = \
            b'arcf' + \
            b'\x00' * 32 + \
            b'\x04\x00\x00\x00' + \
            b'test' + \
            b'\x0b\x00\x00\x00\x00\x00\x00\x00' + \
            b'testcontent' + \
            b'\x03\x00\x00\x00' + \
            b'wow' + \
            b'\x0e\x00\x00\x00\x00\x00\x00\x00' + \
            b'suchgreatstuff'

        self.assertEqual(len(arc), len(expected))
        self.assertEqual(read_all(arc), expected)

    def test_add_one_file_object(self):
        with tempfile.NamedTemporaryFile() as tf:
            tf.write(b'testcontent')
            tf.seek(0)

            arc = Archiver()
            arc.add_file("test", tf)

            expected = \
                b'arcf' + \
                b'\x00' * 32 + \
                b'\x04\x00\x00\x00' + \
                b'test' + \
                b'\x0b\x00\x00\x00\x00\x00\x00\x00' + \
                b'testcontent'

            self.assertEqual(len(arc), len(expected))
            self.assertEqual(read_all(arc), expected)

    def test_add_multiple_file_objects(self):
        with tempfile.NamedTemporaryFile() as tf1, \
             tempfile.NamedTemporaryFile() as tf2:
            tf1.write(b'testcontent')
            tf1.seek(0)
            tf2.write(b'suchgreatstuff')
            tf2.seek(0)

            arc = Archiver()
            arc.add_file("test", tf1)
            arc.add_file("wow", tf2)

            expected = \
                b'arcf' + \
                b'\x00' * 32 + \
                b'\x04\x00\x00\x00' + \
                b'test' + \
                b'\x0b\x00\x00\x00\x00\x00\x00\x00' + \
                b'testcontent' + \
                b'\x03\x00\x00\x00' + \
                b'wow' + \
                b'\x0e\x00\x00\x00\x00\x00\x00\x00' + \
                b'suchgreatstuff'

            self.assertEqual(len(arc), len(expected))
            self.assertEqual(read_all(arc), expected)

    def test_mix_bytes_and_file_object(self):
        with tempfile.NamedTemporaryFile() as tf:
            tf.write(b'suchgreatstuff')
            tf.seek(0)

            arc = Archiver()
            arc.add_file("test", b"testcontent")
            arc.add_file("wow", tf)

            expected = \
                b'arcf' + \
                b'\x00' * 32 + \
                b'\x04\x00\x00\x00' + \
                b'test' + \
                b'\x0b\x00\x00\x00\x00\x00\x00\x00' + \
                b'testcontent' + \
                b'\x03\x00\x00\x00' + \
                b'wow' + \
                b'\x0e\x00\x00\x00\x00\x00\x00\x00' + \
                b'suchgreatstuff'

            self.assertEqual(len(arc), len(expected))
            self.assertEqual(read_all(arc), expected)

    def test_seek_beginning(self):
        with tempfile.NamedTemporaryFile() as tf:
            tf.write(b'suchgreatstuff')
            tf.seek(0)

            arc = Archiver()
            arc.add_file("test", b"testcontent")
            arc.add_file("wow", tf)

            expected = \
                b'arcf' + \
                b'\x00' * 32 + \
                b'\x04\x00\x00\x00' + \
                b'test' + \
                b'\x0b\x00\x00\x00\x00\x00\x00\x00' + \
                b'testcontent' + \
                b'\x03\x00\x00\x00' + \
                b'wow' + \
                b'\x0e\x00\x00\x00\x00\x00\x00\x00' + \
                b'suchgreatstuff'

            for _ in range(10):
                self.assertEqual(len(arc), len(expected))
                self.assertEqual(read_all(arc), expected)
                arc.seek(0)

    def test_seek_middle(self):
        with tempfile.NamedTemporaryFile() as tf:
            tf.write(b'suchgreatstuff')
            tf.seek(0)

            arc = Archiver()
            arc.add_file("test", b"testcontent")
            arc.add_file("wow", tf)

            expected = \
                b'content' + \
                b'\x03\x00\x00\x00' + \
                b'wow' + \
                b'\x0e\x00\x00\x00\x00\x00\x00\x00' + \
                b'suchgreatstuff'

            arc.seek(4 + 32 + 4 + 4 + 8 + 4)
            self.assertEqual(read_all(arc), expected)

    def test_seek_end(self):
        with tempfile.NamedTemporaryFile() as tf:
            tf.write(b'suchgreatstuff')
            tf.seek(0)

            arc = Archiver()
            arc.add_file("test", b"testcontent")
            arc.add_file("wow", tf)

            expected = b''

            arc.seek(4 + 32 + 4 + 4 + 8 + 11 + 4 + 3 + 8 + 14)
            self.assertEqual(read_all(arc), expected)

    def test_seek_beyond_end(self):
        with tempfile.NamedTemporaryFile() as tf:
            tf.write(b'suchgreatstuff')
            tf.seek(0)

            arc = Archiver()
            arc.add_file("test", b"testcontent")
            arc.add_file("wow", tf)

            expected = b''

            arc.seek(100000)
            self.assertEqual(read_all(arc), expected)

    def test_gzip_one_file(self):
        arc = Archiver(use_gzip=True)

        arc.add_file("test", b"testcontent")

        expected = \
            b'arcf' + \
            b'\x01\x00\x00\x00' + \
            b'\x00' * 28 + \
            b'\x04\x00\x00\x00' + \
            b'test' + \
            b'\x1f\x00\x00\x00\x00\x00\x00\x00' + \
            b'\x1f\x8b\x08\x00\x00\x00\x00\x00\x02\xff\x2b\x49\x2d\x2e\x49\xce' + \
            b'\xcf\x2b\x49\xcd\x2b\x01\x00\x04\xd0\x2f\x90\x0b\x00\x00\x00'

        self.assertEqual(len(arc), len(expected))
        self.assertEqual(read_all(arc), expected)

    def test_gzip_mix_bytes_and_file_object(self):
        with tempfile.NamedTemporaryFile() as tf:
            tf.write(b'suchgreatstuff')
            tf.seek(0)

            arc = Archiver(use_gzip=True)
            arc.add_file("test", b"testcontent")
            arc.add_file("wow", tf)

            expected = \
                b'arcf' + \
                b'\x01\x00\x00\x00' + \
                b'\x00' * 28 + \
                b'\x04\x00\x00\x00' + \
                b'test' + \
                b'\x1f\x00\x00\x00\x00\x00\x00\x00' + \
                b'\x1f\x8b\x08\x00\x00\x00\x00\x00\x02\xff\x2b\x49\x2d\x2e\x49\xce' + \
                b'\xcf\x2b\x49\xcd\x2b\x01\x00\x04\xd0\x2f\x90\x0b\x00\x00\x00' + \
                b'\x03\x00\x00\x00' + \
                b'wow' + \
                b'\x22\x00\x00\x00\x00\x00\x00\x00' + \
                b'\x1f\x8b\x08\x00\x00\x00\x00\x00\x02\xff\x2b\x2e\x4d\xce\x48\x2f' + \
                b'\x4a\x4d\x2c\x29\x2e\x29\x4d\x4b\x03\x00\x6b\xb4\xc1\x02\x0e\x00' + \
                b'\x00\x00'

            self.assertEqual(len(arc), len(expected))
            self.assertEqual(read_all(arc), expected)

    def test_gzip_seek_beginning(self):
        with tempfile.NamedTemporaryFile() as tf:
            tf.write(b'suchgreatstuff')
            tf.seek(0)

            arc = Archiver(use_gzip=True)
            arc.add_file("test", b"testcontent")
            arc.add_file("wow", tf)

            expected = \
                b'arcf' + \
                b'\x01\x00\x00\x00' + \
                b'\x00' * 28 + \
                b'\x04\x00\x00\x00' + \
                b'test' + \
                b'\x1f\x00\x00\x00\x00\x00\x00\x00' + \
                b'\x1f\x8b\x08\x00\x00\x00\x00\x00\x02\xff\x2b\x49\x2d\x2e\x49\xce' + \
                b'\xcf\x2b\x49\xcd\x2b\x01\x00\x04\xd0\x2f\x90\x0b\x00\x00\x00' + \
                b'\x03\x00\x00\x00' + \
                b'wow' + \
                b'\x22\x00\x00\x00\x00\x00\x00\x00' + \
                b'\x1f\x8b\x08\x00\x00\x00\x00\x00\x02\xff\x2b\x2e\x4d\xce\x48\x2f' + \
                b'\x4a\x4d\x2c\x29\x2e\x29\x4d\x4b\x03\x00\x6b\xb4\xc1\x02\x0e\x00' + \
                b'\x00\x00'

            for _ in range(10):
                arc.seek(0)
                self.assertEqual(len(arc), len(expected))
                self.assertEqual(read_all(arc), expected)

    def test_lz4_one_file(self):
        arc = Archiver(use_lz4=True)

        arc.add_file("test", b"testcontent")

        expected = \
            b'arcf' + \
            b'\x02\x00\x00\x00' + \
            b'\x00' * 28 + \
            b'\x04\x00\x00\x00' + \
            b'test' + \
            b'\x1e\x00\x00\x00\x00\x00\x00\x00' + \
            b'\x04\x22\x4d\x18\x64\x40\xa7\x0b\x00\x00\x80\x74\x65\x73\x74\x63' + \
            b'\x6f\x6e\x74\x65\x6e\x74\x00\x00\x00\x00\x27\x31\x63\xd5'

        self.assertEqual(len(arc), len(expected))
        self.assertEqual(read_all(arc), expected)

    def test_lz4_one_file_object(self):
        with tempfile.NamedTemporaryFile() as tf:
            tf.write(b'testcontent')
            tf.seek(0)

            arc = Archiver(use_lz4=True)
            arc.add_file("test", tf)

            expected = \
                b'arcf' + \
                b'\x02\x00\x00\x00' + \
                b'\x00' * 28 + \
                b'\x04\x00\x00\x00' + \
                b'test' + \
                b'\x1e\x00\x00\x00\x00\x00\x00\x00' + \
                b'\x04\x22\x4d\x18\x64\x40\xa7\x0b\x00\x00\x80\x74\x65\x73\x74\x63' + \
                b'\x6f\x6e\x74\x65\x6e\x74\x00\x00\x00\x00\x27\x31\x63\xd5'

            self.assertEqual(len(arc), len(expected))
            self.assertEqual(read_all(arc), expected)

    def test_gzip_one_pass_only(self):
        """Checks that a zipped archiver only goes through the file once if
        cache_chunks is given.
        """
        arc = Archiver(use_gzip=True, cache_chunks=True)

        arc.add_file("test", UnseekableFile(b'testcontent'))

        expected = \
            b'arcf' + \
            b'\x01\x00\x00\x00' + \
            b'\x00' * 28 + \
            b'\x04\x00\x00\x00' + \
            b'test' + \
            b'\x1f\x00\x00\x00\x00\x00\x00\x00' + \
            b'\x1f\x8b\x08\x00\x00\x00\x00\x00\x02\xff\x2b\x49\x2d\x2e\x49\xce' + \
            b'\xcf\x2b\x49\xcd\x2b\x01\x00\x04\xd0\x2f\x90\x0b\x00\x00\x00'

        self.assertEqual(len(arc), len(expected))
        self.assertEqual(read_all(arc), expected)

    def test_unzipped_one_pass_only(self):
        """Checks that an unzipped archiver only goes through the file once if
        cache_chunks is given.
        """
        arc = Archiver(cache_chunks=True)

        arc.add_file("test", UnseekableFile(b'testcontent'))

        expected = \
            b'arcf' + \
            b'\x00' * 32 + \
            b'\x04\x00\x00\x00' + \
            b'test' + \
            b'\x0b\x00\x00\x00\x00\x00\x00\x00' + \
            b'testcontent'

        self.assertEqual(len(arc), len(expected))
        self.assertEqual(read_all(arc), expected)


if __name__ == '__main__':
    unittest.main()
