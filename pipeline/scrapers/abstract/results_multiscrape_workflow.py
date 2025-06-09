"""Provides a client that outlines the series of steps
necessary to scrape a search results page of a
development bank for project URLs and data.
"""

from abc import abstractmethod
from pipeline.constants import (
    COMPLETED_STATUS,
    ERROR_STATUS,
    NOT_STARTED_STATUS,
    PROJECT_PARTIAL_PAGE_WORKFLOW
)
from datetime import datetime
from logging import Logger
from pipeline.models.task import TaskUpdate
from pipeline.scrapers.abstract import BaseWorkflow
from pipeline.services.web import DataRequestClient
from pipeline.services.database import DbClient
from pipeline.services.pubsub import PublisherClient
from typing import Dict, List, Tuple


class ResultsMultiScrapeWorkflow(BaseWorkflow):
    """An abstract class to scrape the search results page of a
    generic development bank website (or alternatively, call an
    API endpoint) for *both* project page URLs and project
    data. The URLs are then "queued" for processing by other nodes
    within a larger distributed system, while the project records
    are saved to a database.
    
    This process is necessary when a list of project records and a
    project detailed view each have incomplete information when
    considered separately.
    """

    def __init__(
        self,
        data_request_client: DataRequestClient,
        pubsub_client: PublisherClient,
        db_client: DbClient,
        logger: Logger) -> None:
        """Initializes a new instance of a `ResultsMultiScrapeWorkflow`.

        Args:
            data_request_client (`DataRequestClient`): A client
                for making HTTP GET requests while adding
                random delays and rotating user agent headers.

            pubsub_client (`PubSubClient`): A wrapper client for the 
                Google Cloud Platform Pub/Sub API. Configured to
                publish messages to the appropriate 'tasks' topic.

            db_client (`DbClient`): A client used to insert and
                update tasks in the database.

            logger (`Logger`): An instance of the logging class.

        Returns:
            `None`
        """
        super().__init__(logger)
        self._data_request_client = data_request_client
        self._pubsub_client = pubsub_client
        self._db_client = db_client


    @property
    def next_workflow(self) -> str:
        """The name of the workflow to execute after this
        workflow has finished.
        """
        return PROJECT_PARTIAL_PAGE_WORKFLOW


    @abstractmethod
    def scrape_results_page(self, url: str) -> Tuple[List[str], List[Dict]]:
        """Requests the given development bank project search
        results page and then scrapes all individual project
        URLs and records from that page. Implementation of
        this method differs by bank.

        Args:
            url (`str`): The URL for the results page.

        Returns:
            (list of str, list of dict): A tuple consisting of the
                project page URLs and partial project records.
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

            num_delivery_attempts (`int`): The number of times the
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
            `None`
        """
        # Begin tracking updates for current task
        task_update = TaskUpdate()
        task_update.id = task_id
        task_update.processing_start_utc = datetime.utcnow()
        task_update.retry_count = num_delivery_attempts - 1
        self._logger.info(f"Processing job '{job_id}', source '{source}', "
            f"task '{task_id}', message '{message_id}'.")

        try:
            # Scrape search results page for project URLs and partial records
            try:
                task_update.scraping_start_utc = datetime.utcnow()
                project_page_urls, project_records = self.scrape_results_page(url)
                task_update.scraping_end_utc = datetime.utcnow()
            except Exception as e:
                raise Exception(f"Failed to scrape search results page. {e}")

            # Insert project record(s) into database
            try:
                for r in project_records:
                    r['task_id'] = task_update.id
                self._db_client.bulk_create_staged_projects(project_records)
            except Exception as e:
                raise Exception(f"Failed to insert new project record(s) into database. {e}")

            # Insert new tasks for scraping project pages into database
            try:
                payload = []
                for url in project_page_urls:
                    payload.append({
                        "job_id": job_id,
                        "status": NOT_STARTED_STATUS,
                        "source": source,
                        "url": url,
                        "workflow_type": self.next_workflow
                    })
                project_page_messages = self._db_client.bulk_create_tasks(payload)
            except Exception as e:
                raise Exception("Failed to insert new tasks for scraping "
                    f"project pages into database. {e}")
            
            # Publish task messages to Pub/Sub for other nodes to pick up
            try:
                for msg in project_page_messages:
                    self._pubsub_client.publish_message(msg)
            except Exception as e:
                raise Exception(f"Failed to publish all {len(project_page_messages)} "
                    f"messages to Pub/Sub. {e}")

        except Exception as e:
            # Log error
            error_message = "Results multi-scraping workflow " \
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
