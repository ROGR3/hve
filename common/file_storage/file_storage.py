from dataclasses import dataclass
from os import makedirs, path
from typing import Protocol


@dataclass(frozen=True)
class StoredFile:
    file_uri: str


class FileStorage(Protocol):
    def write(self, file_name: str, content: bytes) -> StoredFile: ...

    def read(self, file_name: str) -> bytes: ...


class LocalFileStorage(FileStorage):
    def write(self, file_name: str, content: bytes) -> StoredFile:
        if path.sep in file_name:
            dirs = file_name.split(path.sep)
            dirs.pop()
            directory = path.join(*dirs)
            makedirs(directory, exist_ok=True)

        with open(file_name, "wb") as file:
            file.write(content)

        return StoredFile(file_name)

    def read(self, file_name: str) -> bytes:
        with open(file_name, "rb") as file:
            return file.read()
