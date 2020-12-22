import json
import logging

import kafka

from finitestate.common.retry_utils import retry
from finitestate.firmware.plugins.storage.aws_adapter import FSAWSStorageAdapter

logger = logging.getLogger(__name__)


class PluginOutputNotifierMixin(object):
    def __init__(self, *args, **kwargs):
        logger.debug(f"Storage adapter is being enriched with mixin: {self.__class__.__name__}")
        self.bootstrap_servers = kwargs.pop("bootstrap_servers")
        self.retries = kwargs.pop("retries", None) or 1
        self.topics = kwargs.pop("topics")
        super().__init__(*args, **kwargs)

    @property
    @retry(on=kafka.errors.NoBrokersAvailable)
    def kafka_producer(self) -> kafka.KafkaProducer:
        """
        Creates and/or returns an instance of a KafkaProducer

        Note: KafkaProducers don't pickle, so it is important that plugins, which run in a ProcessPool, create
        their instance of a KafkaProducer directly, rather than receiving it from the plugin diver (main_aws.py).
        """
        if not hasattr(self, '_kafka_producer'):
            self._kafka_producer = kafka.KafkaProducer(
                bootstrap_servers=self.bootstrap_servers,
                retries=self.retries,
                value_serializer=lambda v: json.dumps(v, separators=(',', ':')).encode('utf-8'),
            )
        return self._kafka_producer

    def store_metadata_bytes(self, file_id: str, output_location: str, data: bytes) -> str:
        etag = super().store_metadata_bytes(file_id=file_id, output_location=output_location, data=data)
        if etag:
            message = {
                "output_location": output_location,
                "file_id": file_id,
                "etag": etag
            }
            self.kafka_producer.send(topic=self.topics["plugin_output_notification"], value=message)
        return etag


class FSEventingAWSStorageAdapter(PluginOutputNotifierMixin, FSAWSStorageAdapter):
    pass
