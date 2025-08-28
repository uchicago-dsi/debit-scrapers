"""Provides wrapper clients for cloud-based task queues."""

# Standard library imports
import json
import time
from abc import ABC, abstractmethod

# Third-party imports
import requests
from django.conf import settings


class MessageQueueClient(ABC):
    """A duck-typed interface for a task queue."""

    @abstractmethod
    def enqueue(self, tasks: list[dict]) -> None:
        """Queues one or more tasks to be processed.

        Args:
            tasks: The tasks.

        Returns:
            `None`
        """
        raise NotImplementedError

    @abstractmethod
    def purge(self, sources: list[str]) -> None:
        """Purges all tasks related to the given data sources.

        Args:
            sources: The data sources.

        Returns:
            `None`
        """
        raise NotImplementedError


class GoogleCloudTaskQueue(MessageQueueClient):
    """A wrapper for Google Cloud Tasks."""

    def __init__(self) -> None:
        """Initializes a new instance of a `GoogleCloudTaskQueue`.

        Raises:
            `RuntimeError` if the Django project is not correctly configured.

        Args:
            `None`

        Returns:
            `None`
        """
        try:
            self._project = settings.GOOGLE_CLOUD_PROJECT_ID
            self._region = settings.GOOGLE_CLOUD_TASKS_QUEUE_REGION
        except AttributeError as e:
            raise RuntimeError(
                f"Django project not correctly configured. {e}"
            ) from None

    def enqueue(self, tasks: list[dict]) -> None:
        """Queues one or more tasks to be processed.

        NOTE: HTTP target endpoints, maximum task concurrency, and
        authentication are configured at the level of the queue
        during the infrastructure build process.

        References:
        - https://cloud.google.com/tasks/docs/creating-http-target-tasks#create_a_task_using_the_buffertask_method

        Args:
            tasks: The tasks.

        Returns:
            `None`
        """
        for task in tasks:
            # Compose URL
            queue = f"{task['source'].lower()}-queue"
            endpoint = (
                f"projects/{self._project}/locations/{self._region}/"
                f"queues/{queue}/tasks:buffer"
            )
            url = f"https://cloudtasks.googleapis.com/v2beta3/{endpoint}"

            # Issue POST request
            r = requests.post(url, json=json.dumps(task).encode(), timeout=60)

            # Raise error if task not queued successfully
            if not r.ok:
                raise RuntimeError(
                    f'Failed to queue task for "{queue}". The call '
                    f'returned a status code of "{r.status_code} - '
                    f'{r.reason}" and the message "{r.text}".'
                )

    def purge(self, sources: list[str]) -> None:
        """Purges all tasks related to the given data sources.

        References:
        - https://cloud.google.com/tasks/docs/manage-queues-and-tasks#purge-tasks

        Args:
            sources: The data sources.

        Returns:
            `None`
        """
        # Purge each queue
        for source in sources:
            # Compose URL
            queue = f"{source.lower()}-queue"
            endpoint = (
                f"projects/{self._project}/locations/{self._region}/"
                f"queues/{queue}/:purge"
            )
            url = f"https://cloudtasks.googleapis.com/v2/{endpoint}"

            # Issue POST request with empty request body
            r = requests.post(url, timeout=60)

            # Raise error if task not queued successfully
            if not r.ok:
                raise RuntimeError(
                    f'Failed to purge tasks for "{queue}". The call '
                    f'returned a status code of "{r.status_code} - '
                    f'{r.reason}" and the message "{r.text}".'
                )

        # Sleep for one minute to ensure purge operations have taken effect
        time.sleep(60)


class TaskQueueFactory:
    """A factory for creating task queues."""

    @staticmethod
    def get() -> MessageQueueClient:
        """Creates a task queue for the current environment."""
        return GoogleCloudTaskQueue()
