"""Provides a client that outlines the series
of steps necessary to scrape a project page
for a development bank and then insert that
project record into a database.
"""

from abc import abstractmethod
from datetime import datetime
from logging import Logger
from scrapers.abstract.base_workflow import BaseWorkflow
from scrapers.constants import COMPLETED_STATUS, ERROR_STATUS
from scrapers.models.task import TaskUpdate
from scrapers.services.data_request import DataRequestClient
from scrapers.services.database import DbClient
from typing import Dict, List


class ProjectScrapeWorkflow(BaseWorkflow):
    """An abstract class to scrape or query project data from
    a development bank given a URL, clean those project
    record(s), and then save the records to a database. 
    """

    def __init__(
        self,
        data_request_client: DataRequestClient,
        db_client: DbClient,
        logger: Logger) -> None:
        """Initializes a new instance of a `ProjectScrapeWorkflow`.

        Args:
            data_request_client (`DataRequestClient`): A client
                for making HTTP GET requests while adding
                random delays and rotating user agent headers.

            db_client (`DbClient`): A client for inserting and
                updating tasks in the database.

            logger (`Logger`): An instance of the logging class.

        Returns:
            None
        """
        super().__init__(logger)
        self._data_request_client = data_request_client
        self._db_client = db_client


    @property
    def next_workflow(self) -> str:
        """The name of the workflow to execute, if any.
        """
        return None


    @abstractmethod
    def scrape_project_page(self, url) -> List[Dict]:
        """Scrapes a website or queries an API endpoint for
        development bank project record(s). Implementation
        of this method differs by bank.

        Args:
            url (`str`): The URL for the results page.

        Returns:
            (`list` of `dict`): The raw record(s).
        """
        raise NotImplementedError


    def execute(
        self,
        message_id: str,
        num_delivery_attempts: int,
        job_id: str,
        task_id: str,
        source: str,
        url: str) -> None:
        """Executes the workflow.

        Args:
            message_id (`str`): The assigned id for the Pub/Sub message.

            num_delivery_attempts (int): The number of times the
                Pub/Sub message has been delivered without being
                acknowledged.

            job_id (`str`): The unique identifier for the processing
                job that encapsulates all data loading, scraping,
                and cleaning tasks.

            task_id (`str`): The unique identifier for the current 
                scraping task.

            source (`str`): The name of the data source to scrape.

            url (`str`): The URL of the page to scrape, if applicable.

        Returns:
            None
        """
        # Begin tracking updates for current task
        task_update = TaskUpdate()
        task_update.id = task_id
        task_update.processing_start_utc = datetime.utcnow()
        task_update.retry_count = num_delivery_attempts - 1
        self._logger.info(f"Processing job '{job_id}', source '{source}', "
            f"task '{task_id}', message '{message_id}'.")

        try:
            # Extract project data
            try:
                task_update.scraping_start_utc = datetime.utcnow()
                project_records = self.scrape_project_page(url)
                task_update.scraping_end_utc = datetime.utcnow()
            except Exception as e:
                raise Exception(f"Failed to scrape project page. {e}")

            # Insert project record(s) into database
            try:
                for r in project_records:
                    r['task_id'] = task_update.id
                self._db_client.bulk_insert_staged_projects(project_records)
            except Exception as e:
                raise Exception(f"Failed to insert project record(s) into database. {e}")
               
        except Exception as e:
            # Log error
            error_message = "Project page scraping workflow " \
                f"failed for message {message_id}. {e}"
            self._logger.error(error_message)

            # Record task failure in database
            task_update.status = ERROR_STATUS
            task_update.last_failed_at_utc = datetime.utcnow()
            task_update.last_error_message = error_message
            self._db_client.update_task(task_update)

            # Bubble up error
            raise Exception(error_message)

        # Record task success in database
        task_update.status = COMPLETED_STATUS
        task_update.processing_end_utc = datetime.utcnow()
        self._db_client.update_task(task_update)

    