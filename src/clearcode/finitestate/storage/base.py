"""
base.py

Base class definitions for all storage adapters for Finite State pipeline (AWS, etc).
"""
from abc import ABC, abstractmethod
from typing import List


class FSStorageAdapterBase(ABC):
    """Base class definitions for all firmware plugins. Handles all overrides for storing.

    None of the `override` functions should be modified, overrides should be handled by this base class.

    `load_overrides` should be called each time a new message is handled, setting the
    `output_locations` and `storage_id` for the message, as well as loading the overrides.
    """
    def __init__(self):
        self._current_storage_id = None
        self._overrides = {}
        self._output_locations = []

    def load_overrides(self, output_locations: List[str], storage_id: str):
        """ Load any overrides that exist, only with the *.jsonl extension (right now).

        Currently not possible to have overrides for other binary types (.tar.gz, etc).

        Should be called each time a new message is handled, setting the `output_locations`
        and `storage_id` for the message, as well as loading the overrides.
        """
        self._current_storage_id = storage_id
        self._overrides = {}
        self._output_locations = output_locations
        for output_location in self._output_locations:
            if self.override_exists(output_location, self._current_storage_id):
                self._overrides[output_location] = self.get_override(output_location, self._current_storage_id)

    def has_overrides(self) -> bool:
        """Returns if any overrides exist, after a call to `load_overrides`."""
        return len(self._overrides) > 0

    def all_overrides(self) -> bool:
        """Returns if overrides exist for all `output_locations`, after a call to `load_overrides`."""
        return self.has_overrides() and all(output_location in self._overrides.keys()
                                            for output_location in self._output_locations)

    def store_overrides(self):
        """Stores any loaded overrides into primary `output_locations` after a call to `load_overrides`."""
        for override_location, override_values in self._overrides.items():
            self.store_metadata(self._current_storage_id, override_location, override_values)

    @abstractmethod
    def file_exists(self, file_id: str) -> bool:
        """Checks if the given file exists in storage.

        Args:
            file_id: SHA256 hash of the file to retrieve.

        Returns:
            True if the file exists in storage, else False.
        """
        raise NotImplementedError()

    @abstractmethod
    def download_file_to_local_path(self, file_id: str, download_directory: str) -> str:
        """Downloads `file_id` to `local_path` on the local filesystem.

        Args:
            file_id: SHA256 hash of the file to retrieve metadata for.
            download_directory: Full path of directory to download the file to. The directory must already exist.

        Returns:
            Full path of file downloaded

        Raises:
            FileNotFoundError: If the requested file does not exist in storage.
            Exception: If any other error occurs while downloading a file.
        """
        raise NotImplementedError()

    @abstractmethod
    def upload_file_from_local_path(self, local_path: str, file_id: str):
        """Uploads `local_path` from the local filesystem to file storage as `file_id`.

        Args:
            local_path: Full path (including filename) of the file to upload to storage.
            file_id: SHA256 hash of the file to upload `local_path` as.

        Raises:
            FileNotFoundError: If the given file does not exist on the local filesystem.
            Exception: If any other error occurs while uploading a file
        """
        raise NotImplementedError()

    @abstractmethod
    def get_file_content(self, file_id: str) -> bytes:
        """Retrieves the complete content of the file with the given ID from
        storage.

        Args:
            file_id: SHA256 hash of the file to retrieve.

        Returns:
            Byte content of `file_id`.

        Raises:
            FileNotFoundError: If file doesn't exist in storage.
            Exception: If any other error occurs while retrieving file content
        """
        raise NotImplementedError()

    @abstractmethod
    def get_file_content_chunk(self, file_id: str, offset: int, max_bytes: int) -> bytes:
        """Retrieves a chunk of content from a file.
        Most typically used to get the header/leading bytes of a file.

        Args:
            file_id: SHA256 hash of the file to retrieve.
            offset: Offset from beginning of file to start retrieval.
            max_bytes: Maximum number of bytes to return.

        Returns:
            Byte content, up to `max_bytes` of `file_id` starting from `offset`.

        Raises:
            FileNotFoundError: If file doesn't exist in storage.
            Exception: If any other error occurs while retrieving file content
        """
        raise NotImplementedError()

    @abstractmethod
    def get_file_size(self, file_id: str) -> int:
        """Retrieves the size of content of the given file (by ID).

        Args:
            file_id: SHA256 hash of the file to get the size of.

        Returns:
            Size of the file in bytes.

        Raises:
            FileNotFoundError: If file doesn't exist in storage.
        """
        raise NotImplementedError()

    @abstractmethod
    def metadata_exists(self, file_id: str, location: str) -> bool:
        """Checks if metadata at `location` exists in storage for `file_id`.

        Args:
            file_id: SHA256 hash of the file to retrieve.
            location: Location to retrieve metadata `file_id` from (AWS = 'folder', PostgreSQL = tablename, etc.).

        Returns:
            True if the metadata exists in storage, else False.
        """
        raise NotImplementedError()

    @abstractmethod
    def download_metadata_to_local_path(self, file_id: str, location: str, download_directory: str) -> str:
        """
        Args:
            file_id: SHA256 hash of the file to retrieve metadata for.
            location: Location to retrieve metadata `file_id` from (AWS = 'folder', PostgreSQL = tablename, etc.).
            download_directory: Full path of directory to download the file to. The directory must already exist.

        Returns:
            Full path of file downloaded

        Raises:
            FileNotFoundError: If the requested metadata does not exist in storage.
        """
        raise NotImplementedError()

    @abstractmethod
    def upload_metadata_from_local_path(self, local_path: str, file_id: str, location: str):
        """Uploads `local_path` from the local filesystem to metadata storage as `file_id`.

        Args:
            local_path: Full path (including filename) of the file to upload to storage.
            file_id: SHA256 hash of the file to upload metadata from `local_path` for.
            location: Location to upload metadata for `file_id` to (AWS = 'folder', PostgreSQL = tablename, etc.).

        Raises:
            FileNotFoundError: If the given file does not exist on the local filesystem.
            Exception: If any other error occurs while uploading metadata
        """
        raise NotImplementedError()

    @abstractmethod
    def get_metadata_bytes(self, file_id: str, location: str) -> bytes:
        """Retrieves raw metadata content about the given file from the
        specified location. It is up to the implementing class to determine
        how to recover the data from the given location string.

        Args:
            file_id: SHA256 hash of the file to retrieve metadata for.
            location: Location to retrieve metadata `file_id` from (AWS = 'folder', PostgreSQL = tablename, etc.).

        Returns:
            Byte content of metadata `file_id`.

        Raises:
            FileNotFoundError: If file doesn't exist.
            Exception: If any other error occurs while retrieving metadata
        """
        raise NotImplementedError()

    @abstractmethod
    def store_metadata_bytes(self, file_id: str, output_location: str, data: bytes) -> str:
        """Stores the given raw metadata content at the specified location for
        the given file. It is up to the implementing class to determine how to
        use the `output_location` and `file_id` value to store the data in a
        deterministic, recoverable place.

        Args:
            file_id: SHA256 hash of the file to store.
            output_location: Location to store metadata bytes (AWS = 'folder', PostgreSQL = tablename, etc.).
            data: Bytes to store.

        Returns:
            String Unique identifier from storage system (AWS = ETag,PostgreSQL = row UUID pk or version,
            local = MD5, etc.)

        Raises:
            Exception: Raises any implementation-specific error.
        """
        raise NotImplementedError()

    @abstractmethod
    def get_metadata(self, file_id: str, location: str) -> List[dict]:
        """Retrieves list-of-dictionary formatted metadata content about the
        given file from the specified location. It is up to the implementing
        class to determine how to recover the data from the given location
        string.

        Args:
            file_id: SHA256 hash of the file to retrieve.
            location: Location to retrieve metadata `file_id` from (AWS = 'folder', PostgreSQL = tablename, etc.).

        Returns:
            List of dictionaries of content of metadata `file_id`.

        Raises:
            FileNotFoundError: If file doesn't exist.
        """
        raise NotImplementedError()

    @abstractmethod
    def store_metadata(self, file_id: str, output_location: str, result: List[dict]):
        """Stores the given list-of-dictionary formatted metadata content at
        the specified location for the given file. It is up to the
        implementing class to determine how to use the `output_location` and
        `file_id` value to store the data in a deterministic, recoverable
        place.

        Args:
            file_id: SHA256 hash of the file to store.
            output_location: Location to store metadata list (AWS = 'folder', PostgreSQL = tablename, etc.).
            result: List of dictionaries to store.

        Raises:
            Exception: Raises any implementation-specific error.
        """
        raise NotImplementedError()

    @abstractmethod
    def plugin_config_exists(self, plugin_name: str, storage_id: str) -> bool:
        """Checks if custom plugin configuration exists for the given `plugin_name`
        and `storage_id`.

        Args:
            plugin_name: Name of the plugin to look for custom configuration for.
            storage_id: Object id for which the custom configuration applies.

        Returns:
            True if custom configuration exists for the given `plugin_name` and
            `storage_id`, else False.
        """
        raise NotImplementedError()

    @abstractmethod
    def get_plugin_config(self, plugin_name: str, storage_id: str) -> dict:
        """Loads any available custom plugin configuration from storage for the given
        `storage_id` and plugin type. Custom configuration is used to override
        or provide additional plugin-specific settings, while still allowing the plugin
        logic to run and produce automated results as normal. An example use of this is
        for providing a custom base address or processor specification to the Ghidra
        plugin for a specific binary file where our normal automated detection methods
        fail for some reason.

        Args:
            plugin_name: Name of the plugin to get custom configuration for.
            storage_id: ID used to identify the results for the triggering
                event. Basically, this will usually be either the file hash or
                firmware hash, or some sort of composite key combining the two.
                Each input message type must specify what value should be used
                to lookup file content and results related to it.

        Returns:
            Dictionary where the keys are configuration setting names and the values
            are the desired configuration values. Keys that do not have custom
            configuration values are ommitted from the dictionary.

        Raises:
            FileNotFoundError: Raised if no custom configuration exists for the
            given `plugin_name` and `storage_id` combination.
        """
        raise NotImplementedError()

    @abstractmethod
    def override_exists(self, output_location: str, storage_id: str) -> bool:
        """Checks if custom plugin configuration exists for the given `output_location`
        and `storage_id`.

        Args:
            output_location: Output location to look for an override for.
            storage_id: Object id for which the override applies.

        Returns:
            True if a manual override exists for the given `output_location` and
            `storage_id`, else False.
        """
        raise NotImplementedError()

    @abstractmethod
    def get_override(self, output_location: str, storage_id: str) -> List[dict]:
        """Loads any available override from storage for the given `storage_id`
        and `output_location`. Overrides are found in storage at a fixed location,
        and are manually generated on a per-output-type-per-object-id basis. They
        are provided so that specific analysis result values can be overridden
        based on outside knowledge acquired through manual analysis.

        Args:
            output_location: Output location to look for an override for.
            storage_id: ID used to identify the results for the triggering
                event. Basically, this will usually be either the file hash or
                firmware hash, or some sort of composite key combining the two.
                Each input message type must specify what value should be used
                to lookup file content and results related to it.

        Returns:
            List of dictionaries representing the full 'manual override' content
            for the given `output_location` and `storage_id` combination.

        Raises:
            FileNotFoundError: Raised if no manaul override exists for the
            given `output_location` and `storage_id` combination.
        """
        raise NotImplementedError()
