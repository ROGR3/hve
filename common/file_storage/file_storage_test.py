import os
import shutil
from unittest import TestCase

from common.file_storage.file_storage import LocalFileStorage


class TestLocalFileStorage(TestCase):
    def setUp(self) -> None:
        self.storage = LocalFileStorage()
        self.test_file = "test_file.txt"
        self.test_content = b"Hello, world!"

    def tearDown(self) -> None:
        if os.path.exists(self.test_file):
            os.remove(self.test_file)

    def test_write_creates_file(self) -> None:
        stored_file = self.storage.write(self.test_file, self.test_content)
        self.assertEqual(stored_file.file_uri, self.test_file)
        self.assertTrue(os.path.exists(self.test_file))

    def test_read_returns_correct_content(self) -> None:
        self.storage.write(self.test_file, self.test_content)
        content = self.storage.read(self.test_file)
        self.assertEqual(content, self.test_content)

    def test_write_creates_nested_directories(self) -> None:
        nested_file = "nested/dir/test_file.txt"
        stored_file = self.storage.write(nested_file, self.test_content)
        self.assertEqual(stored_file.file_uri, nested_file)
        self.assertTrue(os.path.exists(nested_file))

        shutil.rmtree("nested")

    def test_read_nonexistent_file_raises_error(self) -> None:
        with self.assertRaises(FileNotFoundError):
            self.storage.read("nonexistent.txt")
