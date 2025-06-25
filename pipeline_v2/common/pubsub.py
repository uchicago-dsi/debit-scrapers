"""Provides wrapper clients Google Cloud Pub/Sub topics and subscriptions."""

# Standard library imports
import json
from concurrent import futures
from logging import Logger
from typing import Dict, List, Optional

# Third-party imports
from google.api_core import retry
from google.cloud import pubsub_v1
from google.pubsub_v1.types.pubsub import PullResponse, ReceivedMessage


class PublisherClient:
    """A wrapper for the Google Cloud API's PublisherClient."""

    def __init__(
        self,
        project_id: str,
        topic_id: str,
        logger: Logger,
        publish_timeout_in_seconds: Optional[int] = None,
    ) -> None:
        """Initializes a new instance of a `PublisherClient`.

        Documentation:
        - ["Publish messages to topics"](https://cloud.google.com/pubsub/docs/publisher)
        - ["Python Client for Google Cloud Pub / Sub](https://googleapis.dev/python/pubsub/latest/publisher/index.html)
        - ["What is the difference between megabytes and mebibytes?"](https://www.gbmb.org/blog/what-is-the-difference-between-megabytes-and-mebibytes-32)

        Args:
            project_id: The Google Cloud Project id.

            topic_id: The name of the topic to which
                messages will be published.

            logger: An instance of the logger.

            publish_timeout_in_seconds: The number of
                seconds to await a message publish action
                before raising an `Exception`. Defaults to `None`,
                in which case Google's default timeout is used.

        Returns:
            `None`
        """
        try:
            self._logger = logger
            self._publisher = pubsub_v1.PublisherClient()
            self._topic_path = self._publisher.topic_path(project_id, topic_id)
            self._publish_timeout_sec = publish_timeout_in_seconds
        except Exception as e:
            raise Exception(f"Failed to create a new instance of a `PubSubClient`. {e}")

    def publish_message(self, data: Dict) -> None:
        """Publishes a message to the topic. The Pub/Sub client libraries
        automatically batch messages if one of three conditions has
        been reached: (1) 100 messages have been queued for delivery,
        (2) the batch size reaches 1 mebibyte (MiB), or (3) 10 ms
        have passed.

        Args:
            data: The data to publish.

        Returns:
            `None`
        """
        try:
            # Encode data
            data_str = json.dumps(data)
            encoded_data: bytes = data_str.encode("utf-8")

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
                    raise Exception("Message publishing timed out.")

            # Set callback function
            publish_future.add_done_callback(callback)

        except Exception as e:
            raise RuntimeError(
                f'Failed to publish Pub/Sub message for "{data_str}". {e}'
            ) from None


class SubscriberClient:
    """A wrapper for the Google Cloud API's SubscriberClient."""

    def __init__(
        self,
        project_id: str,
        subscription_id: str,
        msg_batch_size: int,
        msg_retry_deadline: int,
        logger: Logger,
    ) -> None:
        """Initializes a new instance of a `SubscriberClient`.

        Args:
            project_id: The Google Cloud Project id.

            subscription_id: The name of the subscription
                from which messages will be pulled.

            msg_batch_size: The maximum number of
                messages to pull at once from the subscription.

            msg_retry_deadline: The number of seconds to
                retry subscription pulls if an error occurs.

            logger: An instance of the logger.

        Returns:
            `None`
        """
        self._msg_batch_size = msg_batch_size
        self._msg_retry_deadline = msg_retry_deadline
        self._subscriber = pubsub_v1.SubscriberClient()
        self._subscription_path = self._subscriber.subscription_path(
            project=project_id, subscription=subscription_id
        )
        self._logger = logger

    def acknowledge_message(self, msg: ReceivedMessage) -> None:
        """Acknowledges a message to prevent it from being
        delivered again to the subscription.

        Documentation:
        - ["Class Message (2.18.4)"](https://cloud.google.com/python/docs/reference/pubsub/latest/google.cloud.pubsub_v1.subscriber.message.Message)

        Args:
            msg: The message.

        Returns:
            `None`
        """
        self._subscriber.acknowledge(
            request={"subscription": self._subscription_path, "ack_ids": [msg.ack_id]}
        )

    def pull(self) -> List[ReceivedMessage]:
        """Pulls the next batch of messages from the subscription,
        if any exist.

        Documentation:
        - ["google.cloud.pubsub_v1.subscriber.client.Client"](https://cloud.google.com/python/docs/reference/pubsub/latest/google.cloud.pubsub_v1.subscriber.client.Client)
        - ["google.cloud.pubsub_v1.types.PullResponse"](https://cloud.google.com/python/docs/reference/pubsub/latest/google.cloud.pubsub_v1.types.PullResponse)

        Args:
            `None`

        Returns:
            The messages.
        """
        response: PullResponse = self._subscriber.pull(
            request={
                "subscription": self._subscription_path,
                "max_messages": self._msg_batch_size,
            },
            retry=retry.Retry(deadline=self._msg_retry_deadline),
        )
        return response.received_messages
