import collections
import json
import logging
from abc import ABC
from enum import Enum
from typing import BinaryIO, List, TextIO, Union
from uuid import UUID

import jsonlines

from .objects import get_size

logger = logging.getLogger(__name__)


class JsonClassSerializable(ABC, json.JSONEncoder):
    """Base class that allows child classes to serialize to and from pure JSON strings."""
    def default(self, obj):
        """Helper method for json.dumps to parse recursive structures."""
        if isinstance(obj, collections.abc.Set):
            return dict(_set_object=list(obj))
        elif isinstance(obj, UUID):
            return obj.hex
        elif not hasattr(obj, '__dict__'):
            return dict(obj)
        elif isinstance(obj, Enum):
            return str(obj)
        else:
            return obj.__dict__

    def serialize(self):
        """Serializes all classes inheriting this to a JSON string."""
        return json.dumps(self, cls=JsonClassSerializable)

    @staticmethod
    def deserialize(json_string: str):
        """Creates an instance of this class from a JSON string representation.

        Examples:
            Implementing a deserialize method on a child class YourChildClass::

                return YourChildClass(**json.loads(json_string))

            Instantiating YourChildClass from a json_string then becomes::

                your_class = YourChildClass.deserialize(json_string)
            """
        raise NotImplementedError()


def write_jsonl(entities: List[dict], fp: Union[BinaryIO, TextIO], max_size: int = None, default=None):
    """
    Converts the provided list of dicts to jsonl and writes to the file handle
    :param entities: The list of dicts to be converted to jsonl
    :param fp: The file handle to which the output should be written
    :param max_size: A per-line size limit - jsonl lines over this size will be dropped from the output
    :param default: A function that should return a serializable version of the dict
    """
    def bytes_serializer(obj):
        if isinstance(obj, bytes):
            return obj.decode('utf8', errors='backslashreplace')
        elif isinstance(obj, dict):
            return json.dumps(obj, separators=(',', ':'), default=default)
        raise TypeError(repr(obj) + ' is not JSONL serializable')

    # Convert the result dictionary of metadata into serialized bytes and then store them
    with jsonlines.Writer(fp, compact=True, flush=True, dumps=bytes_serializer) as writer:
        for obj in entities:
            # Verify that the object isn't over a maximum size limit
            if max_size is not None:
                obj_size = get_size(obj)
                if obj_size > max_size:
                    logger.warning(f'Dropping result that is over size limit ({obj_size} bytes > {max_size} byte limit)')
                    continue

            writer.write(obj)
