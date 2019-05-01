import unittest
import io

from arc.unarchiver import Unarchiver


# TODO: DRY
def read_all(file, chunk_size=8192):
    content = b''
    while True:
        chunk = file.read(chunk_size)
        if len(chunk) == 0:
            break
        content += chunk
    return content


class TestUnarchiver(unittest.TestCase):
    def test_one_file(self):
        content = \
            b'arcf' + \
            b'\x00' * 32 + \
            b'\x04\x00\x00\x00' + \
            b'test' + \
            b'\x0b\x00\x00\x00\x00\x00\x00\x00' + \
            b'testcontent'

        buf = io.BytesIO(content)
        unarc = Unarchiver(buf)

        files = unarc.files()
        self.assertEqual(files[0][0], 'test')
        self.assertEqual(read_all(files[0][1]), b'testcontent')

    def test_multiple_files(self):
        content = \
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

        buf = io.BytesIO(content)
        unarc = Unarchiver(buf)

        files = unarc.files()
        self.assertEqual(files[0][0], 'test')
        self.assertEqual(read_all(files[0][1]), b'testcontent')
        self.assertEqual(files[1][0], 'wow')
        self.assertEqual(read_all(files[1][1]), b'suchgreatstuff')

    def test_gzip_one_file(self):
        content = \
            b'arcf' + \
            b'\x01\x00\x00\x00' + \
            b'\x00' * 28 + \
            b'\x04\x00\x00\x00' + \
            b'test' + \
            b'\x1f\x00\x00\x00\x00\x00\x00\x00' + \
            b'\x1f\x8b\x08\x00\x00\x00\x00\x00\x02\xff\x2b\x49\x2d\x2e\x49\xce' + \
            b'\xcf\x2b\x49\xcd\x2b\x01\x00\x04\xd0\x2f\x90\x0b\x00\x00\x00'

        buf = io.BytesIO(content)
        unarc = Unarchiver(buf)

        files = unarc.files()
        self.assertEqual(files[0][0], 'test')
        self.assertEqual(read_all(files[0][1]), b'testcontent')

    def test_lz4_one_file(self):
        content = \
            b'arcf' + \
            b'\x02\x00\x00\x00' + \
            b'\x00' * 28 + \
            b'\x04\x00\x00\x00' + \
            b'test' + \
            b'\x1e\x00\x00\x00\x00\x00\x00\x00' + \
            b'\x04\x22\x4d\x18\x64\x40\xa7\x0b\x00\x00\x80\x74\x65\x73\x74\x63' + \
            b'\x6f\x6e\x74\x65\x6e\x74\x00\x00\x00\x00\x27\x31\x63\xd5'

        buf = io.BytesIO(content)
        unarc = Unarchiver(buf)

        files = unarc.files()
        self.assertEqual(files[0][0], 'test')
        self.assertEqual(read_all(files[0][1]), b'testcontent')


if __name__ == '__main__':
    unittest.main()
