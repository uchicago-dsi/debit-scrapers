"""Project Scrape Workflow

Defines the steps necessary to extract project records
from a development bank project webpage and then insert
those records into a database. Implements the template
design pattern to allow customization by subclasses.
"""

# Standard library imports
from abc import abstractmethod
from datetime import UTC, datetime
from logging import Logger

# Application imports
from common.http import DataRequestClient
from extract.dal import DatabaseClient
from extract.domain import TaskUpdateRequest
from extract.models import ExtractionTask
from extract.workflows.abstract import BaseWorkflow


class ProjectScrapeWorkflow(BaseWorkflow):
    """Base class for all project scrape workflows.

    Defines a template pattern in which one or more
    project records are scraped from a website or
    queried from an API and then persisted to a database.
    Handles data persistence and status updates in the
    database. Subclasses implement logic specific to
    the data source.

    This workflow differs from `ProjectDownloadWorkflow` in
    that project records exist as separate resources to
    be fetched.
    """

    def __init__(
        self,
        data_request_client: DataRequestClient,
        db_client: DatabaseClient,
        logger: Logger,
    ) -> None:
        """Initializes a new instance of a `ProjectScrapeWorkflow`.

        Args:
            data_request_client: A client for making HTTP GET requests
                while adding random delays and rotating user agent headers.

            db_client: A client for inserting and updating
                tasks in the database.

            logger: A standard logger instance.

        Returns:
            `None`
        """
        super().__init__(logger)
        self._data_request_client = data_request_client
        self._db_client = db_client

    @property
    def next_workflow(self) -> str:
        """The name of the workflow to execute, if any."""
        return None

    @abstractmethod
    def scrape_project_page(self, url: str) -> list[dict]:
        """Scrapes a website or queries an API for project record(s).

        Args:
            url: The URL to the resource with project data.

        Returns:
            The raw record(s).
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

            task_id: The unique identifier for the current scraping task.

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
            # Extract project data
            try:
                project_records = self.scrape_project_page(url)
            except Exception as e:
                raise RuntimeError(f"Failed to scrape project page. {e}") from None

            # Insert project record(s) into database
            try:
                for project in project_records:
                    project["task_id"] = task_id
                self._db_client.bulk_create_staged_projects(project_records)
            except Exception as e:
                raise RuntimeError(
                    f"Failed to insert project record(s) into database. {e}"
                ) from None

        except Exception as e:
            # Log error
            error_message = (
                f"Project page scraping workflow failed for message {message_id}. {e}"
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
