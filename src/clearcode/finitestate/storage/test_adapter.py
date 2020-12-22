"""
test_adapter.py

Class definitions for test storage adapter. Raises errors and returns none if not used with mock.
"""
from .base import FSStorageAdapterBase
from typing import List


class FSTestStorageAdapter(FSStorageAdapterBase):
    """Class used as a stand-in during testing.
    Nothing is written anywhere, and `FileNotFoundError` is raised unless
    specific mocks are put in place during test execution.
    """

    # -------------------
    # File-based methods

    def file_exists(self, file_id: str) -> bool:
        return False

    def download_file_to_local_path(self, file_id: str, local_path: str):
        raise FileNotFoundError()

    def upload_file_from_local_path(self, local_path: str, file_id: str):
        raise FileNotFoundError()

    def get_file_content(self, file_id: str) -> bytes:
        raise FileNotFoundError()

    def get_file_content_chunk(self, file_id: str, offset: int, max_bytes: int) -> bytes:
        raise FileNotFoundError()

    def get_file_size(self, file_id: str) -> int:
        raise FileNotFoundError()

    # -----------------------
    # Metadata-based methods

    def metadata_exists(self, file_id: str, location: str) -> bool:
        return False

    def download_metadata_to_local_path(self, file_id: str, location: str, local_path: str):
        raise FileNotFoundError()

    def upload_metadata_from_local_path(self, local_path: str, file_id: str):
        raise FileNotFoundError()

    def get_metadata_bytes(self, file_id: str, location: str) -> bytes:
        raise FileNotFoundError()

    def store_metadata_bytes(self, file_id: str, output_location: str, data: bytes) -> str:
        raise FileNotFoundError()

    def get_metadata(self, file_id: str, location: str) -> List[dict]:
        raise FileNotFoundError()

    def store_metadata(self, file_id: str, output_location: str, result: List[dict]):
        raise FileNotFoundError()

    # -----------------------
    # Config-loading methods

    def plugin_config_exists(self, plugin_name: str, storage_id: str) -> bool:
        return False

    def get_plugin_config(self, plugin_name: str, storage_id: str) -> dict:
        raise FileNotFoundError()

    # -------------------------
    # Override-loading methods

    def override_exists(self, output_location: str, storage_id: str) -> bool:
        return False

    def get_override(self, output_location: str, storage_id: str) -> List[dict]:
        raise FileNotFoundError()
