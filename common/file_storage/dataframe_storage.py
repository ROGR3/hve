from io import BytesIO

from polars import DataFrame, read_parquet

from common.file_storage.file_storage import FileStorage, StoredFile


class ArrowPolarsDataframeStorage:
    def __init__(self, file_storage: FileStorage, path: str) -> None:
        self.__file_storage = file_storage
        self.__path = path

    def write(self, file_name: str, data: DataFrame) -> StoredFile:
        buffer_value = self.__convert_dataframe_to_bytes(data)
        file_path = self.__generate_file_path(file_name)
        return self.__file_storage.write(file_path, buffer_value)

    def read(self, file_name: str) -> DataFrame:
        file_path = self.__generate_file_path(file_name)
        file_content = self.__file_storage.read(file_path)
        with BytesIO(file_content) as buffer:
            return read_parquet(buffer, use_pyarrow=True)

    def __generate_file_path(self, file_name: str) -> str:
        return (
            self.__path + file_name + "_polars.arrow.gz" if not file_name.endswith("_polars.arrow.gz") else self.__path + file_name
        )

    def __convert_dataframe_to_bytes(self, data: DataFrame) -> bytes:
        with BytesIO() as buffer:
            data.write_parquet(buffer, use_pyarrow=True)
            return buffer.getvalue()
