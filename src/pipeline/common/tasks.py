"""Provides wrapper clients for cloud-based task queues."""

# Standard library imports
import json
import time
from abc import ABC, abstractmethod

# Third-party imports
import google.auth
from django.conf import settings
from google.auth.transport.requests import AuthorizedSession

# Application imports
from common.logger import LoggerFactory


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
    def purge(self) -> None:
        """Purges all tasks from the configured queues.

        Args:
            sources: The data sources.

        Returns:
            `None`
        """
        raise NotImplementedError


class DummyQueue(MessageQueueClient):
    """A dummy task queue that serves as a pass-through."""

    def __init__(self) -> None:
        """Initializes a new instance of a `DummyQueue`.

        Args:
            `None`

        Returns:
            `None`
        """
        self._logger = LoggerFactory.get("DUMMY QUEUE")

    def enqueue(self, tasks: list[dict]) -> None:
        """Queues one or more tasks to be processed.

        Args:
            tasks: The tasks.

        Returns:
            `None`
        """
        self._logger.info(f"Queueing {len(tasks)} tasks.")
        self._logger.info(json.dumps(tasks))

    def purge(self) -> None:
        """Purges all tasks from the configured queues.

        Args:
            `None`

        Returns:
            `None`
        """
        self._logger.info("Purging tasks for all data sources.")


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
        # Validate existence of Google Cloud Project settings
        try:
            self._project = settings.GOOGLE_CLOUD_PROJECT_ID
            self._region = settings.GOOGLE_CLOUD_PROJECT_REGION
        except AttributeError as e:
            raise RuntimeError(
                f"Django project not correctly configured. {e}"
            ) from None

        # List project queues
        try:
            self._queues = self.list_names()
        except Exception as e:
            raise RuntimeError(
                f'Failed to list queues for project "{self._project}". {e}'
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
        # Configure default authentication credentials
        creds, _ = google.auth.default(
            scopes=["https://www.googleapis.com/auth/cloud-platform"]
        )
        session = AuthorizedSession(creds)

        for task in tasks:
            # Look up queue name by data source
            queue = self.get_name(task["source"])

            # Compose URL
            url = f"https://cloudtasks.googleapis.com/v2beta3/{queue}/tasks:buffer"

            # Issue POST request
            r = session.post(url, json=json.dumps(task).encode(), timeout=60)

            # Raise error if task not queued successfully
            if not r.ok:
                raise RuntimeError(
                    f'Failed to queue task for "{task["source"]}". The '
                    f'call returned a status code of "{r.status_code} - '
                    f'{r.reason}" and the message "{r.text}".'
                )

    def get_name(self, source: str) -> str:
        """Fetches the name of the queue for the given data source.

        NOTE: Names are fully-qualified and have the format:
        `projects/PROJECT_ID/locations/LOCATION_ID/queues/QUEUE_ID`

        Args:
            source: The data source.

        Returns:
            The queue name.
        """
        matches = [
            queue for queue in self._queues if source.lower() in queue.lower()
        ]
        if len(matches) > 1:
            raise RuntimeError(f'Multiple queues found for source "{source}".')
        elif len(matches) == 0:
            raise RuntimeError(f'Queue not found for source "{source}".')
        else:
            return matches[0]

    def list_names(self) -> list[str]:
        """Fetches the names of all queues in the current project..

        NOTE: Names are fully-qualified and have the format:
        `projects/PROJECT_ID/locations/LOCATION_ID/queues/QUEUE_ID`

        References:
        - https://cloud.google.com/tasks/docs/reference/rest/v2/projects.locations.queues/list
        - https://cloud.google.com/tasks/docs/reference/rest/v2/projects.locations.queues#Queue

        Args:
            `None`

        Returns:
            The names.
        """
        # Configure default authentication credentials
        creds, _ = google.auth.default(
            scopes=["https://www.googleapis.com/auth/cloud-platform"]
        )
        session = AuthorizedSession(creds)

        # Issue GET request
        url = (
            "https://cloudtasks.googleapis.com/v2/projects/"
            f"{self._project}/locations/{self._region}/queues"
        )
        r = session.get(url, timeout=60)

        # Raise error if task not queued successfully
        if not r.ok:
            raise RuntimeError(
                f'Failed to list queues for project "{self._project}". '
                f'The call returned a status code of "{r.status_code} - '
                f'{r.reason}" and the message "{r.text}".'
            )

        # Return names
        return [queue["name"] for queue in r.json()["queues"]]

    def purge(self) -> None:
        """Purges all tasks from the configured queues.

        References:
        - https://cloud.google.com/tasks/docs/manage-queues-and-tasks#purge-tasks

        Args:
            `None`

        Returns:
            `None`
        """
        # Configure default authentication credentials
        creds, _ = google.auth.default(
            scopes=["https://www.googleapis.com/auth/cloud-platform"]
        )
        session = AuthorizedSession(creds)

        # Purge each queue
        for queue in self._queues:
            # Compose URL
            url = f"https://cloudtasks.googleapis.com/v2/{queue}/:purge"

            # Issue POST request with empty request body
            r = session.post(url, timeout=60)

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
        if settings.DEBUG:
            return DummyQueue()
        else:
            return GoogleCloudTaskQueue()
