import unittest
import io

from unarchiver import Unarchiver


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


if __name__ == '__main__':
    unittest.main()
