import unittest
import tempfile

from archive import Archive


def read_all(file, chunk_size=8192):
    content = b''
    while True:
        chunk = file.read(chunk_size)
        if len(chunk) == 0:
            break
        content += chunk
    return content


class TestStringMethods(unittest.TestCase):
    def test_empty(self):
        arc = Archive()

        expected = b'arcf' + b'\x00' * 32

        self.assertEqual(len(arc), len(expected))
        self.assertEqual(read_all(arc), expected)

    def test_one_file(self):
        arc = Archive()

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
        arc = Archive()

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

            arc = Archive()
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

            arc = Archive()
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

            arc = Archive()
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

            arc = Archive()
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

            for i in range(10):
                self.assertEqual(len(arc), len(expected))
                self.assertEqual(read_all(arc), expected)
                arc.seek(0)

    def test_seek_middle(self):
        with tempfile.NamedTemporaryFile() as tf:
            tf.write(b'suchgreatstuff')
            tf.seek(0)

            arc = Archive()
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

            arc = Archive()
            arc.add_file("test", b"testcontent")
            arc.add_file("wow", tf)

            expected = b''

            arc.seek(4 + 32 + 4 + 4 + 8 + 11 + 4 + 3 + 8 + 14)
            self.assertEqual(read_all(arc), expected)

    def test_seek_beyond_end(self):
        with tempfile.NamedTemporaryFile() as tf:
            tf.write(b'suchgreatstuff')
            tf.seek(0)

            arc = Archive()
            arc.add_file("test", b"testcontent")
            arc.add_file("wow", tf)

            expected = b''

            arc.seek(100000)
            self.assertEqual(read_all(arc), expected)


if __name__ == '__main__':
    unittest.main()
