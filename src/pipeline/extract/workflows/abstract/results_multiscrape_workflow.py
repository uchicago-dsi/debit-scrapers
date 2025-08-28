"""Results Multi-Scrape Workflow

Defines the steps necessary to extract project URLs
and project data simultaneously from a development bank
resource. Implements the template design pattern to allow
customization by subclasses.
"""

# Standard library imports
from abc import abstractmethod
from datetime import UTC, datetime
from logging import Logger

# Third-party imports
from django.conf import settings

# Application imports
from common.http import DataRequestClient
from common.tasks import MessageQueueClient
from extract.dal import DatabaseClient
from extract.domain import TaskUpdateRequest
from extract.models import ExtractionTask
from extract.workflows.abstract import BaseWorkflow


class ResultsMultiScrapeWorkflow(BaseWorkflow):
    """Base class for results multi-scrape workflows.

    Defines a template pattern in which a search results page
    of a generic development bank website is scraped (or
    alternatively, an API endpoint is queried) for *both* project
    page URLs and project data. The URLs are then "queued" for
    processing while the project records are saved to a database.
    This process is necessary when a list of project records and a
    project detail view each have incomplete information when
    considered separately.

    This class handles data persistence and status updates in
    the database as well as completion of messaging tasks.
    Subclasses implement logic specific to the data source.
    """

    def __init__(
        self,
        data_request_client: DataRequestClient,
        msg_queue_client: MessageQueueClient,
        db_client: DatabaseClient,
        logger: Logger,
    ) -> None:
        """Initializes a new instance of a `ResultsMultiScrapeWorkflow`.

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

    @property
    def next_workflow(self) -> str:
        """The workflow to execute after this workflow has finished."""
        return settings.PROJECT_PARTIAL_PAGE_WORKFLOW

    @abstractmethod
    def scrape_results_page(self, url: str) -> tuple[list[str], list[dict]]:
        """Scrapes a result page for project URLs and partial records.

        Args:
            url: The URL for the results page.

        Returns:
            A tuple consisting of the project page URLs
                and partial project records.
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
            # Scrape search results page for project URLs and partial records
            try:
                project_page_urls, project_records = self.scrape_results_page(url)
            except Exception as e:
                raise RuntimeError(
                    f"Failed to scrape search results page. {e}"
                ) from None

            # Insert project record(s) into database
            try:
                for project in project_records:
                    project["task_id"] = task_id
                self._db_client.bulk_create_staged_projects(project_records)
            except Exception as e:
                raise RuntimeError(
                    f"Failed to insert new project record(s) into database. {e}"
                ) from None

            # Insert new tasks for scraping project pages into database
            try:
                payload = []
                for url in project_page_urls:
                    payload.append(
                        {
                            "job_id": job_id,
                            "source": source,
                            "url": url,
                            "workflow_type": self.next_workflow,
                        }
                    )
                project_page_messages = self._db_client.bulk_upsert_tasks(payload)
            except Exception as e:
                raise RuntimeError(
                    "Failed to insert new tasks for scraping "
                    f"project pages into database. {e}"
                ) from None

            # Enqueue task messages to be handled in subsequent requests
            try:
                for msg in project_page_messages:
                    self._msg_queue_client.enqueue(msg)
            except Exception as e:
                raise RuntimeError(
                    f"Failed to enqueue all {len(project_page_messages)} messages. {e}"
                ) from None

        except Exception as e:
            # Log error
            error_message = (
                f"Results multi-scraping workflow failed for message {message_id}. {e}"
            )
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
