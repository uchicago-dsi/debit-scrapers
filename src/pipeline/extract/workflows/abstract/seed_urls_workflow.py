"""Provides a client that generates the initial set of URLs for data extraction."""

# Standard library imports
from abc import abstractmethod
from datetime import UTC, datetime
from logging import Logger

# Application imports
from common.http import DataRequestClient
from common.tasks import MessageQueueClient
from extract.dal import DatabaseClient
from extract.domain import TaskUpdateRequest
from extract.models import ExtractionTask
from extract.workflows.abstract import BaseWorkflow


class SeedUrlsWorkflow(BaseWorkflow):
    """Base class for seed URLs workflows.

    An abstract class that generates the initial set of URLs
    for web scraping or API querying and then "queues" those
    URLs for processing. Handles data persistence and status
    updates in the database as well as completion of messaging
    tasks. Subclasses implement logic specific to the data source.
    """

    def __init__(
        self,
        data_request_client: DataRequestClient,
        msg_queue_client: MessageQueueClient,
        db_client: DatabaseClient,
        logger: Logger,
    ) -> None:
        """Initializes a new instance of a `SeedUrlsWorkflow`.

        Args:
            data_request_client: A client for making HTTP GET requests
                while adding random delays and rotating user agent headers.

            msg_queue_client: A client for a message queue.

            db_client: A client used to insert and
                update tasks in the database.

            logger: A standard logger instance.

        Returns:
            `None`
        """
        super().__init__(logger)
        self._data_request_client = data_request_client
        self._msg_queue_client = msg_queue_client
        self._db_client = db_client

    @abstractmethod
    def generate_seed_urls(self) -> list[str]:
        """Generates the first set of URLs to scrape or query.

        Args:
            None

        Returns:
            The URLs.
        """
        raise NotImplementedError

    def execute(
        self,
        message_id: str,
        num_delivery_attempts: int,
        job_id: str,
        task_id: str,
        source: str,
        url: str,
    ) -> None:
        """Executes the workflow.

        Args:
            message_id: The assigned id for the message.

            num_delivery_attempts: The number of times the
                message has been delivered without success.

            job_id: The unique identifier for the processing
                job that encapsulates all data loading, scraping,
                and cleaning tasks.

            task_id: The unique identifier for the current
                scraping task.

            source: The name of the data source to scrape.

            url: The URL of the page to scrape, if applicable.

        Returns:
            `None`
        """
        # Begin tracking updates for current task
        task = TaskUpdateRequest()
        task["id"] = task_id
        task["status"] = ExtractionTask.StatusChoices.IN_PROGRESS
        task["started_at_utc"] = datetime.now(UTC)
        task["retry_count"] = num_delivery_attempts - 1
        self._logger.info(
            f"Processing job '{job_id}', source '{source}', "
            f"task '{task_id}', message '{message_id}'."
        )

        try:
            # Generate initial set of URLs
            try:
                urls = self.generate_seed_urls()
            except Exception as e:
                raise RuntimeError(f"Failed to generate seed urls. {e}") from None

            # Insert new tasks for scraping URLs into database
            try:
                payload = []
                for url in urls:
                    payload.append(
                        {
                            "job_id": job_id,
                            "source": source,
                            "url": url,
                            "workflow_type": self.next_workflow,
                        }
                    )
                created_tasks = self._db_client.bulk_upsert_tasks(payload)
            except Exception as e:
                raise RuntimeError(
                    f"Failed to insert new scraping tasks into database. {e}"
                ) from None

            # Enqueue task messages to be handled in subsequent requests
            try:
                for msg in created_tasks:
                    self._msg_queue_client.enqueue(msg)
            except Exception as e:
                raise RuntimeError(
                    f"Failed to enqueue all {len(created_tasks)} messages. {e}"
                ) from None

        except Exception as e:
            # Log error
            error_message = f"Seed URLs workflow failed for message {message_id}. {e}"
            self._logger.error(error_message)

            # Record task failure in database
            task["status"] = ExtractionTask.StatusChoices.ERROR
            task["failed_at_utc"] = datetime.now(UTC)
            task["last_error"] = error_message
            self._db_client.update_task(task)

            # Bubble up error
            raise RuntimeError(error_message) from None

        # Record task success in database
        task["status"] = ExtractionTask.StatusChoices.COMPLETED
        task["completed_at_utc"] = datetime.now(UTC)
        self._db_client.update_task(task)
