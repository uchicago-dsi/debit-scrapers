"""Provides a client that creates Google Cloud
Pub/Sub messages for a topic.

References:
(1) https://cloud.google.com/pubsub/docs/publisher
(2) https://googleapis.dev/python/pubsub/latest/publisher/index.html
(3) https://www.gbmb.org/blog/what-is-the-difference-between-megabytes-and-mebibytes-32
"""

import json
from concurrent import futures
from google.cloud import pubsub_v1
from logging import Logger
from typing import Dict


class PubSubClient():
    """A wrapper for the Google Cloud API's PublisherClient.
    """

    def __init__(
        self,
        logger: Logger,
        project_id: str,
        topic_id: str,
        publish_timeout_in_seconds: int=None) -> None:
        """Initializes a new instance of a `PubSubClient`.

        Args:
            logger (`Logger`): An instance of the logger.

            project_id (str): The Google Cloud Project id.

            topic_id (str): The name of the topic to which
                messages will be published.

            publish_timeout_in_seconds (int): The number of
                seconds to await a message publish action
                before raising an Exception.

        Returns:
            None
        """
        try:
            self._logger = logger
            self._publisher = pubsub_v1.PublisherClient()
            self._topic_path = self._publisher.topic_path(project_id, topic_id)
            self._publish_timeout_sec = publish_timeout_in_seconds
        except Exception as e:
            raise Exception(f"Failed to create a new "
                            f"instance of a `PubSubClient`. {e}")

        
    def publish_message(self, data: Dict) -> None:
        """Publishes a message to the topic. The Pub/Sub client libraries
        automatically batch messages if one of three conditions has
        been reached: (1) 100 messages have been queued for delivery,
        (2) the batch size reaches 1 mebibyte (MiB), or (3) 10 ms
        have passed.

        Args:
            data (dict): The data to publish.

        Returns:
            None
        """
        try:
            # Encode data
            data_str = json.dumps(data)
            encoded_data: bytes = data_str.encode('utf-8')

            # Initiate message publishing
            publish_future = self._publisher.publish(self._topic_path, encoded_data)
            
            # Define callback function to execute when publishing completes
            def callback(future):
                try:
                    if self._publish_timeout_sec:
                        future.result(timeout=self._publish_timeout_sec)
                    else:
                        future.result()
                except futures.TimeoutError:
                    raise Exception(f"Message publishing timed out.")

            # Set callback function
            publish_future.add_done_callback(callback)

        except Exception as e:
            raise Exception(f"Failed to publish Pub/Sub message for '{data_str}'. {e}")
