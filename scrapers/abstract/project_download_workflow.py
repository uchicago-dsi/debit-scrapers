"""Provides a client that outlines the series of steps
necessary to download project data for a development bank.
"""

import json
import pandas as pd
from abc import abstractmethod
from datetime import datetime
from logging import Logger
from scrapers.abstract.base_workflow import BaseWorkflow
from scrapers.constants import COMPLETED_STATUS, ERROR_STATUS
from scrapers.models.task import TaskUpdate
from scrapers.services.database import DbClient
from scrapers.services.data_request import DataRequestClient


class ProjectDownloadWorkflow(BaseWorkflow):
    """An abstract class to download project records directly
    from a development bank website and then clean and
    save the data to a database.
    """

    def __init__(
        self,
        data_request_client: DataRequestClient,
        db_client: DbClient,
        logger: Logger) -> None:
        """Initializes a new instance of a `ProjectDownloadWorkflow`.

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
    @abstractmethod
    def download_url(self) -> str:
        """The URL containing all project records.
        """
        raise NotImplementedError
    
    
    @property
    def next_workflow(self) -> str:
        """The name of the workflow to execute, if any.
        """
        return None


    @abstractmethod
    def get_projects(self) -> pd.DataFrame:
        """Retrieves all development bank projects through direct
        download and parses them into a Pandas DataFrame.

        Args:
            None
        
        Returns:
            (`pd.DataFrame`): The raw project records.
        """
        raise NotImplementedError

    
    @abstractmethod
    def clean_projects(self, df: pd.DataFrame) -> pd.DataFrame:
        """Cleans project records to conform to an expected schema.

        Args:
            df (`pd.DataFrame`): The raw project records.

        Returns:
            (`pd.DataFrame`): The cleaned records.
        """
        raise NotImplementedError


    def execute(
        self,
        message_id: str,
        num_delivery_attempts: int,
        job_id: str,
        task_id: str,
        source: str,
        url: str=None) -> None:
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
        try:
            # Begin tracking updates for current task
            task_update = TaskUpdate()
            task_update.id = task_id
            task_update.processing_start_utc = datetime.utcnow()
            task_update.retry_count = num_delivery_attempts - 1
            self._logger.info(f"Processing job '{job_id}', source '{source}', "
                f"task '{task_id}', message '{message_id}'.")

            # Download and clean project records
            raw_project_df = self.get_projects()
            clean_project_df = self.clean_projects(raw_project_df)
            
            # Insert project records into database in batches
            try:
                clean_project_df['task_id'] = task_update.id
                json_str = clean_project_df.to_json(orient='records')
                clean_project_records = json.loads(json_str)
                self._db_client.bulk_insert_staged_projects(clean_project_records)
            except Exception as e:
                raise Exception(f"Failed to insert new project records into database. {e}")
        
        except Exception as e:
            # Log error
            error_message = f"Project download task failed for message '{message_id}'. {e}"
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
