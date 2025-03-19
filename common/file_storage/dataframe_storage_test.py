from io import BytesIO
from unittest import TestCase
from unittest.mock import Mock

from polars import DataFrame

from common.file_storage.dataframe_storage import ArrowPolarsDataframeStorage
from common.file_storage.file_storage import FileStorage


class TestArrowPolarsDataframeStorage(TestCase):
    def setUp(self) -> None:
        self.__df = DataFrame({"a": [1, 2, 3]})
        self.__storage_mock = Mock(FileStorage)
        self.__test_path = "/dummy/path/"
        self.__polars_df_storage = ArrowPolarsDataframeStorage(self.__storage_mock, self.__test_path)

    def test_write_calls_file_storage_write(self):
        self.__storage_mock.write.return_value = "dummy_stored_file"

        result = self.__polars_df_storage.write("testfile", self.__df)

        self.__storage_mock.write.assert_called_once()
        call_args, _ = self.__storage_mock.write.call_args
        self.assertEqual(call_args[0], self.__test_path + "testfile_polars.arrow.gz")
        self.assertIsInstance(call_args[1], bytes)
        self.assertGreater(len(call_args[1]), 0)

        self.assertEqual(result, "dummy_stored_file")

    def test_read_calls_file_storage_read_and_returns_dataframe(self):
        self.__mock_read_return_value()

        result_df = self.__polars_df_storage.read("testfile")

        self.__storage_mock.read.assert_called_once_with(self.__test_path + "testfile_polars.arrow.gz")
        self.assertTrue(result_df.equals(self.__df))

    def test_file_path_handling_when_filename_already_suffixed(self):
        self.__mock_read_return_value()

        result_df = self.__polars_df_storage.read("file_polars.arrow.gz")

        self.__storage_mock.read.assert_called_once_with(self.__test_path + "file_polars.arrow.gz")
        self.assertTrue(result_df.equals(self.__df))

    def __mock_read_return_value(self) -> None:
        with BytesIO() as buf:
            self.__df.write_parquet(buf, use_pyarrow=True)
            parquet_bytes = buf.getvalue()
        self.__storage_mock.read.return_value = parquet_bytes
