"""
aws_adapter.py

Class definition for AWS S3-based storage of both files and metadata.
"""
import io
import json
import logging
import os
import sys
from typing import List

from .s3 import (download_file_from_s3, get_s3_file_size, retrieve_data_from_s3,
                                         retrieve_data_chunk_from_s3, s3_key_exists, upload_data_to_s3, upload_file_to_s3)
from .json_utils import write_jsonl
from .base import FSStorageAdapterBase

logger = logging.getLogger(__name__)


class FSAWSStorageAdapter(FSStorageAdapterBase):
    """AWS storage adapter class.
    This adapter should implements all the boto3-specific storage routines to
    S3. Metadata is stored in a 'Metadata' bucket, and files are stored in a
    'Files' bucket.

    For additional documentation of this class's API, refer to the base class.
    """

    # Maximum size of a single-line JSON-formatted analysis result entry.
    # Anything larger than this will be dropped, and a warning message is logged.
    # This is done to prevent any single entry from blowing up downstream processing.
    MAX_ANALYSIS_RESULT_SIZE = 1 * 1024 * 1024

    # Map of 'location' (aka 'analysis result data type') to 'extension' used to store the metadata.
    # The extension will default to '.jsonl', so only include things that are _not_ JSONL.
    METADATA_LOCATION_EXTENSION_MAP = {
        'binary_analysis/ghidra_database': 'tar.gz',
        'ground_truth_upload_metadata': 'json',
        'sbom/unified': 'json',
        'unpack_failed': 'json',
        'unpack_report': 'json',
    }

    def __init__(self, file_bucket, metadata_bucket):
        super().__init__()
        logger.debug(f'Initializing AWS storage adapter')
        self.file_bucket = file_bucket
        self.metadata_bucket = metadata_bucket

        logger.debug(f'File bucket: {self.file_bucket}')
        logger.debug(f'Metadata bucket: {self.metadata_bucket}')

    def _generate_s3_metadata_key(self, file_id: str, location: str) -> str:
        """Generates the S3 key required to retrieve the given metadata.

        Args:
            file_id: SHA256 hash of the file to retrieve.
            location: Location to retrieve metadata `file_id` from (aka the 'folder' where the metadata is found).

        Returns:
            Full S3 key to the given metadata.
        """
        return f'{location}/{file_id}.{self._get_metadata_location_extension(location)}'

    def _get_metadata_location_extension(self, location: str) -> str:
        """Returns the extension used to store metadata at the given location.

        Args:
            location: Location of relevant metadata (aka 'analysis data type')

        Returns:
            Extension to use for the given location.
        """
        if location in self.METADATA_LOCATION_EXTENSION_MAP:
            # Return the proper extension if it is specified explicitly
            return self.METADATA_LOCATION_EXTENSION_MAP[location]
        elif location.startswith('plugin_configuration/'):
            # All plugin configurations are normal JSON
            return 'json'
        elif location.startswith('manual_overrides/'):
            # All manual overrides use the same extension as whatever they are overriding
            return self._get_metadata_location_extension(location.replace('manual_overrides/', ''))
        else:
            # Default to jsonl
            return 'jsonl'

    # ---------------------
    # Base class overrides

    # File-related functions

    def file_exists(self, file_id: str) -> bool:
        return s3_key_exists(self.file_bucket, file_id)

    def download_file_to_local_path(self, file_id: str, download_directory: str) -> str:
        if not self.file_exists(file_id):
            raise FileNotFoundError(f'File {file_id} not found in storage')
        # Create a local path at which the file will be stored
        local_path = os.path.join(download_directory, file_id)
        download_file_from_s3(self.file_bucket, file_id, local_path)
        return local_path

    def upload_file_from_local_path(self, local_path: str, file_id: str):
        if not os.path.exists(local_path) or not os.path.isfile(local_path):
            raise FileNotFoundError(f'No valid file found to upload at {local_path}')
        upload_file_to_s3(local_path, self.file_bucket, file_id)

    def get_file_content(self, file_id: str) -> bytes:
        if not self.file_exists(file_id):
            raise FileNotFoundError(f'File {file_id} not found in storage')
        return retrieve_data_from_s3(self.file_bucket, file_id)

    def get_file_content_chunk(self, file_id: str, offset: int, max_bytes: int) -> bytes:
        if not self.file_exists(file_id):
            raise FileNotFoundError(f'File {file_id} not found in storage')
        return retrieve_data_chunk_from_s3(self.file_bucket, file_id, offset, max_bytes)

    def get_file_size(self, file_id: str) -> int:
        if not self.file_exists(file_id):
            raise FileNotFoundError(f'File {file_id} not found in storage')
        return get_s3_file_size(self.file_bucket, file_id)

    # Metadata-related functions

    def metadata_exists(self, file_id: str, location: str) -> bool:
        return s3_key_exists(self.metadata_bucket, self._generate_s3_metadata_key(file_id, location))

    def download_metadata_to_local_path(self, file_id: str, location: str, download_directory: str) -> str:
        if not self.metadata_exists(file_id, location):
            raise FileNotFoundError(f'Metadata does not exist for {file_id} at {location}')
        # Create a local path at which the file will be stored
        extension = self._get_metadata_location_extension(location)
        local_path = os.path.join(download_directory, f'{file_id}{f".{extension}" if len(extension) > 0 else ""}')
        download_file_from_s3(self.metadata_bucket, self._generate_s3_metadata_key(file_id, location), local_path)
        return local_path

    def upload_metadata_from_local_path(self, local_path: str, file_id: str, location: str):
        if not os.path.exists(local_path) or not os.path.isfile(local_path):
            raise FileNotFoundError(f'No valid file found to upload at {local_path}')
        upload_file_to_s3(local_path, self.metadata_bucket, self._generate_s3_metadata_key(file_id, location))

    def get_metadata_bytes(self, file_id: str, location: str) -> bytes:
        if not self.metadata_exists(file_id, location):
            raise FileNotFoundError(f'Metadata does not exist for {file_id} at {location}')
        return retrieve_data_from_s3(self.metadata_bucket, self._generate_s3_metadata_key(file_id, location))

    def store_metadata_bytes(self, file_id: str, output_location: str, data: bytes) -> str:
        # Don't write 0-byte files (do nothing)
        if data and len(data) > 0:
            # Store the content into the metadata bucket
            s3_key = self._generate_s3_metadata_key(file_id, output_location)
            logger.info(f'Saving analysis result to s3://{self.metadata_bucket}/{s3_key}')
            try:
                return upload_data_to_s3(self.metadata_bucket, s3_key, data).e_tag.replace('"', '')
            except Exception as e:
                logger.exception(f'An exception occurred when saving to s3://{self.metadata_bucket}/{s3_key}')
                raise e
        else:
            logger.debug('Ignoring 0-byte metadata')

    def get_metadata(self, file_id: str, location: str) -> List[dict]:
        """Retrieves metadata for `file_id` from `location` as a native list of
        dictionaries. For more info refer to the base class docs.

        Returns:
            Metadata at `location` for `file_id` as a list of dictionaries.

        Raises:
            FileNotFoundError: If no metadata exists at `location` for the given `file_id.
            TypeError: If the given location does not contain JSONL-type data.
        """
        # Verify that the given location is used for JSONL data
        if self._get_metadata_location_extension(location) != 'jsonl':
            raise TypeError(f'Metadata at location {location} is not JSONL, and therefore cannot be retrieved by this method.')

        # Retrieve the JSONL bytes and then deserialize them into a native list of dicts
        jsonl_bytes = self.get_metadata_bytes(file_id, location)
        return [json.loads(x) for x in list(filter(None, jsonl_bytes.decode('utf-8').split('\n')))]

    def store_metadata(self, file_id: str, output_location: str, result: List[dict]):
        """Stores the given analysis `result` (list of dictionaries) to the
        metadata bucket after turning it into a JSONL string. The result is
        written to the metadata bucket at the `output_location`. For more info
        refer to the base class docs.

        Any single result in the given list is dropped if it is larger than
        the configured `MAX_ANALYSIS_RESULT_SIZE`.

        Raises:
            TypeError: If the given output_location does not accept JSONL-type
                       data, or if the given `result` is not JSONL-serializable.
            Exception: If any other error occurs while storing metadata.
        """
        # Verify that the given location is used for JSONL data
        if self._get_metadata_location_extension(output_location) != 'jsonl':
            raise TypeError(
                f'Metadata at location {output_location} is not JSONL, and therefore cannot be stored by this method.')

        with io.StringIO() as jsonl_string:
            write_jsonl(result, fp=jsonl_string, max_size=self.MAX_ANALYSIS_RESULT_SIZE)
            self.store_metadata_bytes(file_id, output_location, jsonl_string.getvalue())

    # Config-loading methods

    def plugin_config_exists(self, plugin_name: str, storage_id: str) -> bool:
        # Plugin config is stored in the metadata bucket, so just check at the right location
        return self.metadata_exists(storage_id, f'plugin_configuration/{plugin_name}')

    def get_plugin_config(self, plugin_name: str, storage_id: str) -> dict:
        # Plugin configs are stored as JSON and might have newlines (aka they
        # might be pretty printed), so they have to be deserialized as such.
        return json.loads(self.get_metadata_bytes(storage_id, f'plugin_configuration/{plugin_name}'))

    # Override-loading methods

    def override_exists(self, output_location: str, storage_id: str) -> bool:
        # Overrides are stored in the metadata bucket, so just check at the right location
        return self.metadata_exists(storage_id, f'manual_overrides/{output_location}')

    def get_override(self, output_location: str, storage_id: str) -> List[dict]:
        return self.get_metadata(storage_id, f'manual_overrides/{output_location}')
