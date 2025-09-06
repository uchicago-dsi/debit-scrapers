"""Project Partial Scrape Workflow

Defines the steps necessary to extract select project fields
from a development bank resource and then update the corresponding
project database record with the additional information. Implements
the template design pattern to allow customization by subclasses.
"""

# Standard library imports
from abc import abstractmethod
from logging import Logger

# Application imports
from common.http import DataRequestClient
from extract.dal import DatabaseClient
from extract.workflows.abstract import BaseWorkflow


class ProjectPartialScrapeWorkflow(BaseWorkflow):
    """Base class for all project partial scrape workflows.

    Defines a template pattern in which project data is
    scraped from a webpage (or queried from an API) and then
    cleaned while new URLs are generated and queued to
    extract additional data. Handles data persistence
    and status updates in the database. Subclasses implement
    logic specific to the data source.
    """

    def __init__(
        self,
        data_request_client: DataRequestClient,
        db_client: DatabaseClient,
        logger: Logger,
    ) -> None:
        """Initializes a new instance of a `ProjectPartialScrapeWorkflow`.

        Args:
            data_request_client: A client for making HTTP GET requests
                while adding random delays and rotating user agent headers.

            db_client: A client used to insert and
                update tasks in the database.

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
        """Scrapes a website or queries an API endpoint for project records.

        Args:
            url: The URL for the project page.

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

            # Extract project data
            try:
                project_records = self.scrape_project_page(url)
            except Exception as e:
                raise RuntimeError(
                    f"Failed to scrape project page. {e}"
                ) from None

            # Update project record(s) in database
            try:
                for project in project_records:
                    self._db_client.patch_project(
                        {"task_id": task_id, **project}
                    )
            except Exception as e:
                raise RuntimeError(
                    f"Failed to update project record(s) in database. {e}"
                ) from None

        except Exception as e:
            # Mark task as failed in database
            self._logger.error(f"Task failed. {e}")
            self._db_client.mark_task_failure(
                task_id, str(e), num_delivery_attempts
            )
            raise RuntimeError(str(e)) from None

        # Mark task as successful in database
        self._logger.info("Task succeeded.")
        self._db_client.mark_task_success(task_id, num_delivery_attempts)
