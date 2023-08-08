"""Provides a client that generates the initial set of URLs for web scraping.
"""

from abc import abstractmethod
from datetime import datetime
from logging import Logger
from scrapers.abstract.base_workflow import BaseWorkflow
from scrapers.constants import (
    COMPLETED_STATUS,
    ERROR_STATUS,
    NOT_STARTED_STATUS
)
from scrapers.models.task import TaskUpdate
from scrapers.services.database import DbClient
from scrapers.services.pubsub import PubSubClient
from typing import List


class SeedUrlsWorkflow(BaseWorkflow):
    """An abstract class that generates the initial
    set of URLs for web scraping or API querying
    and then "queues" those URLs for processing by
    other nodes within a larger distributed system.
    """

    def __init__(
        self,
        pubsub_client: PubSubClient,
        db_client: DbClient,
        logger: Logger) -> None:
        """Initializes a new instance of a `SeedUrlsWorkflow`.

        Args:
            pubsub_client (`PubSubClient`): A wrapper client for the 
                Google Cloud Platform Pub/Sub API. Configured to
                publish messages to the appropriate 'tasks' topic.

            db_client (`DbClient`): A client used to insert and
                update tasks in the database.

            logger (`Logger`): An instance of the logging class.

        Returns:
            None
        """
        super().__init__(logger)
        self._pubsub_client = pubsub_client
        self._db_client = db_client


    @abstractmethod
    def generate_seed_urls(self) -> List[str]:
        """Generates the first set of URLs to scrape or query.

        Args:
            None

        Returns:
            (list of str): The URLs.
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
            message_id (str): The assigned id for the Pub/Sub message.

            num_delivery_attempts (int): The number of times the
                Pub/Sub message has been delivered without being
                acknowledged.

            job_id (str): The unique identifier for the processing
                job that encapsulates all data loading, scraping,
                and cleaning tasks.

            task_id (str): The unique identifier for the current 
                scraping task.

            source (str): The name of the data source to scrape.

            url (str): The URL of the page to scrape, if applicable.

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
            # Generate initial set of URLs
            try:
                urls = self.generate_seed_urls()
            except Exception as e:
                raise Exception(f"Failed to generate seed urls. {e}")

            # Insert new tasks for scraping URLs into database
            try:
                payload = []
                for url in urls:
                    payload.append({
                        "job_id": job_id,
                        "status": NOT_STARTED_STATUS,
                        "source": source,
                        "url": url,
                        "workflow_type": self.next_workflow
                    })
                tasks = self._db_client.bulk_insert_tasks(payload)
            except Exception as e:
                raise Exception("Failed to insert new scraping tasks "
                    f"into database. {e}")

            # Publish task messages to Pub/Sub for other nodes to pick up
            try:
                for task in tasks:
                    self._pubsub_client.publish_message(task)
            except Exception as e:
                raise Exception(f"Failed to publish all {len(tasks)} "
                    f"messages to Pub/Sub. {e}")
        
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
