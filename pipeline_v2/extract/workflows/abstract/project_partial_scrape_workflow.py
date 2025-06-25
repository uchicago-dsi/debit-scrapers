"""Defines the steps necessary to extract select project fields
from a development bank resource and then update the corresponding
project database record with the additional information. Implements
the template design pattern to allow customization by subclasses.
"""

# Standard library imports
from abc import abstractmethod
from datetime import datetime, timezone
from logging import Logger
from typing import Dict, List

# Application imports
from common.web import DataRequestClient
from extract.dal import ExtractionDbClient
from extract.domain import TaskUpdateRequest
from extract.models import ExtractionTask
from extract.workflows.abstract import BaseWorkflow


class ProjectPartialScrapeWorkflow(BaseWorkflow):
    """An abstract class to scrape or query project data from
    a development bank given a URL, clean the project
    record(s), and then update the corresponding project
    record(s) in a database.
    """

    def __init__(
        self,
        data_request_client: DataRequestClient,
        db_client: ExtractionDbClient,
        logger: Logger,
    ) -> None:
        """Initializes a new instance of a `ProjectPartialScrapeWorkflow`.

        Args:
            data_request_client: A client for making HTTP GET requests
                while adding random delays and rotating user agent headers.

            db_client: A client for inserting and
                updating tasks in the database.

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
    def scrape_project_page(self, url) -> List[Dict]:
        """Scrapes a website or queries an API endpoint
        for one or more development bank project record(s).
        Implementation of this method differs by bank.

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
            message_id: The assigned id for the Pub/Sub message.

            num_delivery_attempts: The number of times the
                Pub/Sub message has been delivered without being
                acknowledged.

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
        task["started_at_utc"] = datetime.now(timezone.utc)
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
                raise Exception(f"Failed to scrape project page. {e}")

            # Update project record(s) in database
            try:
                for r in project_records:
                    self._db_client.update_staged_project(r)
            except Exception as e:
                raise Exception(f"Failed to update project record(s) in database. {e}")

        except Exception as e:
            # Log error
            error_message = (
                f"Project page scraping workflow failed for message {message_id}. {e}"
            )
            self._logger.error(error_message)

            # Record task failure in database
            task["status"] = ExtractionTask.StatusChoices.ERROR
            task["failed_at_utc"] = datetime.now(timezone.utc)
            task["last_error"] = error_message
            self._db_client.update_task(task)

            # Bubble up error
            raise Exception(error_message)

        # Record task success in database
        task["status"] = ExtractionTask.StatusChoices.COMPLETED
        task["completed_at_utc"] = datetime.now(timezone.utc)
        self._db_client.update_task(task)
