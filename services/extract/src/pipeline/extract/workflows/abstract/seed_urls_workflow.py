"""Provides a client that generates the initial set of URLs for data extraction."""

# Standard library imports
from abc import abstractmethod
from logging import Logger

# Application imports
from common.http import DataRequestClient
from common.tasks import MessageQueueClient
from extract.dal import DatabaseClient
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
        try:
            # Log start of task processing
            self._logger.info(
                f"Processing message {message_id} for task {task_id}. "
                f"Number of delivery attempts: {num_delivery_attempts}"
            )

            # Mark task as pending in database
            self._db_client.mark_task_pending(task_id)

            # Generate initial set of URLs
            try:
                urls = self.generate_seed_urls()
            except Exception as e:
                raise RuntimeError(f"Failed to generate seed urls. {e}") from None

            # Insert new tasks for scraping URLs into database
            try:
                new_tasks = self._db_client.bulk_upsert_tasks(
                    [
                        {
                            "job_id": job_id,
                            "source": source,
                            "url": url,
                            "workflow_type": self.next_workflow,
                        }
                        for url in urls
                    ]
                )
            except Exception as e:
                raise RuntimeError(
                    f"Failed to insert new scraping tasks into database. {e}"
                ) from None

            # Enqueue task messages to be handled in subsequent requests
            try:
                self._msg_queue_client.enqueue(new_tasks)
            except Exception as e:
                raise RuntimeError(
                    f"Failed to enqueue all {len(new_tasks)} messages. {e}"
                ) from None

        except Exception as e:
            # Mark task as failed in database
            self._logger.error(f"Task failed. {e}")
            self._db_client.mark_task_failure(task_id, str(e), num_delivery_attempts)
            raise RuntimeError(str(e)) from None

        # Mark task as successful in database
        self._logger.info("Task succeeded.")
        self._db_client.mark_task_success(task_id, num_delivery_attempts)
