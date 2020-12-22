"""
local_adapter.py

Class definition for local filesystem storage of both files and metadata.
This is mainly intended to be used when running services locally for
development and debugging.
"""
import hashlib
import io
import json
import jsonlines
import logging
import os
from pathlib import Path
from shutil import copyfile
from typing import List

from .objects import get_size
from .base import FSStorageAdapterBase

logger = logging.getLogger(__name__)


class FSLocalStorageAdapter(FSStorageAdapterBase):
    """Local storage adapter class.
    This adapter stores and retrieves all content from the local filesystem.
    Metadata is stored in a 'Metadata' directory, and files are stored in a
    'Files' directory.

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
        'unpack_report': 'json'
    }

    def __init__(self, file_dir, metadata_dir):
        super().__init__()
        logger.debug(f'Initializing local filesystem storage adapter')
        self.file_dir = file_dir
        self.metadata_dir = metadata_dir
        logger.debug(f'File dir: {self.file_dir}')
        logger.debug(f'Metadata dir: {self.metadata_dir}')

    def _generate_metadata_path(self, file_id: str, location: str) -> str:
        """Generates the path required to retrieve the given metadata.

        Args:
            file_id: SHA256 hash of the file to retrieve.
            location: Location to retrieve metadata `file_id` from (aka the 'folder' where the metadata is found).

        Returns:
            Full path to the given metadata.
        """
        return os.path.join(self.metadata_dir, location, f'{file_id}.{self._get_metadata_location_extension(location)}')

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

    def _get_bytes_from_file(self, path: str, offset: int = 0, max_bytes: int = None) -> bytes:
        """Loads up to `max_bytes` from the given file starting from `offset`.
        if `max_bytes` is None, all bytes are loaded starting from offset.
        """
        CHUNK_SIZE = 256 * 1024
        ret_val = b''
        with open(path, 'rb') as f:
            f.seek(offset)
            read_size = CHUNK_SIZE if max_bytes is None else min(CHUNK_SIZE, max_bytes - len(ret_val))
            logger.info(f"READ SIZE: {read_size}")
            data = f.read(read_size)
            if data:
                ret_val += data
            while data:
                read_size = CHUNK_SIZE if max_bytes is None else min(CHUNK_SIZE, max_bytes - len(ret_val))
                logger.info(f"READ SIZE: {read_size}")
                data = f.read(read_size)
                if data:
                    ret_val += data
        return ret_val

    # ---------------------
    # Base class overrides

    # File-related functions

    def file_exists(self, file_id: str) -> bool:
        path = os.path.join(self.file_dir, file_id)
        return os.path.exists(path) and os.path.isfile(path)

    def download_file_to_local_path(self, file_id: str, download_directory: str) -> str:
        if not self.file_exists(file_id):
            raise FileNotFoundError(f'File {file_id} not found in storage')
        # Create a local path at which the file will be stored
        local_path = os.path.join(download_directory, file_id)
        copyfile(os.path.join(self.file_dir, file_id), local_path)
        return local_path

    def upload_file_from_local_path(self, local_path: str, file_id: str):
        dest_path = os.path.join(self.file_dir, file_id)
        # Safely create any parent directories if necessary
        Path(os.path.dirname(dest_path)).mkdir(parents=True, exist_ok=True)
        if not os.path.exists(local_path) or not os.path.isfile(local_path):
            raise FileNotFoundError(f'No valid file found to upload at {local_path}')
        copyfile(local_path, dest_path)

    def get_file_content(self, file_id: str) -> bytes:
        if not self.file_exists(file_id):
            raise FileNotFoundError(f'File {file_id} not found in storage')
        return self._get_bytes_from_file(os.path.join(self.file_dir, file_id))

    def get_file_content_chunk(self, file_id: str, offset: int, max_bytes: int) -> bytes:
        if not self.file_exists(file_id):
            raise FileNotFoundError(f'File {file_id} not found in storage')
        return self._get_bytes_from_file(os.path.join(self.file_dir, file_id), offset, max_bytes)

    def get_file_size(self, file_id: str) -> int:
        if not self.file_exists(file_id):
            raise FileNotFoundError(f'File {file_id} not found in storage')
        return os.path.getsize(os.path.join(self.file_dir, file_id))

    # Metadata-related functions

    def metadata_exists(self, file_id: str, location: str) -> bool:
        path = os.path.join(self.metadata_dir, self._generate_metadata_path(file_id, location))
        return os.path.exists(path) and os.path.isfile(path)

    def download_metadata_to_local_path(self, file_id: str, location: str, download_directory: str) -> str:
        if not self.metadata_exists(file_id, location):
            raise FileNotFoundError(f'Metadata does not exist for {file_id} at {location}')
        # Create a local path at which the file will be stored
        extension = self._get_metadata_location_extension(location)
        local_path = os.path.join(download_directory, f'{file_id}{f".{extension}" if len(extension) > 0 else ""}')
        copyfile(os.path.join(self.metadata_dir, self._generate_metadata_path(file_id, location)), local_path)
        return local_path

    def upload_metadata_from_local_path(self, local_path: str, file_id: str, location: str):
        dest_path = os.path.join(self.metadata_dir, self._generate_metadata_path(file_id, location))
        # Safely create any parent directories if necessary
        Path(os.path.dirname(dest_path)).mkdir(parents=True, exist_ok=True)
        if not os.path.exists(local_path) or not os.path.isfile(local_path):
            raise FileNotFoundError(f'No valid file found to upload at {local_path}')
        copyfile(local_path, dest_path)

    def get_metadata_bytes(self, file_id: str, location: str) -> bytes:
        if not self.metadata_exists(file_id, location):
            raise FileNotFoundError(f'Metadata does not exist for {file_id} at {location}')
        return self._get_bytes_from_file(os.path.join(self.metadata_dir, self._generate_metadata_path(file_id, location)))

    def store_metadata_bytes(self, file_id: str, output_location: str, data: bytes) -> str:
        # Don't write 0-byte files (do nothing)
        if data and len(data) > 0:
            # Store the content into the metadata directory
            dest_path = os.path.join(self.metadata_dir, self._generate_metadata_path(file_id, output_location))
            logger.info(f'Saving analysis result to {dest_path}')
            try:
                # Safely create any parent directories if necessary
                Path(os.path.dirname(dest_path)).mkdir(parents=True, exist_ok=True)
                with open(dest_path, 'wb') as f:
                    f.write(data)
                    return hashlib.md5(data).hexdigest()
            except Exception as e:
                logger.exception(f'An exception occured when saving to {dest_path}')
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
        metadata directory after turning it into a JSONL string. The result is
        written to the metadata directory at the `output_location`. For more info
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

        # Helper private method used to serialize data in a more-robust-than-default way
        def bytes_serializer(obj):
            if isinstance(obj, bytes):
                return obj.decode('utf8', errors='backslashreplace')
            elif isinstance(obj, dict):
                return json.dumps(obj, separators=(',', ':'))
            raise TypeError(repr(obj) + ' is not JSONL serializable')

        # Convert the result dictionary of metadata into serialized bytes and then store them
        with io.StringIO("") as jsonl_string:
            with jsonlines.Writer(jsonl_string, compact=True, flush=True, dumps=bytes_serializer) as writer:
                for obj in result:
                    # Verify that the object isn't over a maximum size limit
                    obj_size = get_size(obj)
                    if obj_size < self.MAX_ANALYSIS_RESULT_SIZE:
                        writer.write(obj)
            self.store_metadata_bytes(file_id, output_location, jsonl_string.getvalue().encode('utf8'))

    # Config-loading methods

    def plugin_config_exists(self, plugin_name: str, storage_id: str) -> bool:
        # Plugin config is stored in the metadata directory, so just check at the right location
        return self.metadata_exists(storage_id, f'plugin_configuration/{plugin_name}')

    def get_plugin_config(self, plugin_name: str, storage_id: str) -> dict:
        # Plugin configs are stored as JSON and might have newlines (aka they
        # might be pretty printed), so they have to be deserialized as such.
        return json.loads(self.get_metadata_bytes(storage_id, f'plugin_configuration/{plugin_name}'))

    # Override-loading methods

    def override_exists(self, output_location: str, storage_id: str) -> bool:
        # Overrides are stored in the metadata directory, so just check at the right location
        return self.metadata_exists(storage_id, f'manual_overrides/{output_location}')

    def get_override(self, output_location: str, storage_id: str) -> List[dict]:
        return self.get_metadata(storage_id, f'manual_overrides/{output_location}')
