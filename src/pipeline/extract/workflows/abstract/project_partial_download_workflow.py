"""Project Partial Download Workflow

Defines the steps necessary to download project data
for a development bank and queue URLs for additional
processing. Implements the template design pattern to
allow customization by subclasses.
"""

# Standard library imports
import json
from abc import abstractmethod
from logging import Logger

# Third-party imports
from django.conf import settings
import pandas as pd

# Application imports
from common.http import DataRequestClient
from common.tasks import MessageQueueClient
from extract.dal import DatabaseClient
from extract.workflows.abstract import BaseWorkflow


class ProjectPartialDownloadWorkflow(BaseWorkflow):
    """Base class for all project partial download workflows.

    Defines a template pattern in which project records are
    directly downloaded from a development bank website or
    queried from an API. The data is persisted to a database
    and used to construct URLs to project resources with
    additional data. The class handles database and message
    queue updates while subclasses implement logic
    specific to the data source.
    """

    def __init__(
        self,
        data_request_client: DataRequestClient,
        msg_queue_client: MessageQueueClient,
        db_client: DatabaseClient,
        logger: Logger,
    ) -> None:
        """Initializes a new instance of a `ProjectDownloadWorkflow`.

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
    @abstractmethod
    def download_url(self) -> str:
        """The URL containing all project records."""
        raise NotImplementedError

    @property
    def next_workflow(self) -> str:
        """The name of the workflow to execute, if any."""
        return settings.PROJECT_PARTIAL_PAGE_WORKFLOW

    @abstractmethod
    def get_projects(self) -> pd.DataFrame:
        """Fetches project records and reads them into a Pandas DataFrame.

        Args:
            `None`

        Returns:
            The raw project records.
        """
        raise NotImplementedError

    @abstractmethod
    def clean_projects(
        self, df: pd.DataFrame
    ) -> tuple[list[str], pd.DataFrame]:
        """Cleans project records and parses the next set of URLs to crawl.

        Args:
            df: The raw project records.

        Returns:
            A two-item tuple consisting of the new URLs and cleaned records.
        """
        raise NotImplementedError

    def execute(
        self,
        message_id: str,
        num_delivery_attempts: int,
        job_id: str,
        task_id: str,
        source: str,
        url: str | None = None,
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
        # Define task retry count (one less than delivery attempt count)
        retry_count = num_delivery_attempts - 1

        try:
            # Log start of task processing
            self._logger.info(
                f"Processing message {message_id} for task {task_id}. "
                f"Number of delivery attempts: {num_delivery_attempts}"
            )

            # Mark task as pending in database
            self._db_client.mark_task_pending(task_id)

            # Download and clean project records
            raw_project_df = self.get_projects()
            urls, clean_project_df = self.clean_projects(raw_project_df)

            # Insert project records into database in batches
            try:
                clean_project_df["task_id"] = task_id
                json_str = clean_project_df.to_json(orient="records")
                clean_projects = json.loads(json_str)
                self._db_client.bulk_upsert_projects(clean_projects)
            except Exception as e:
                raise RuntimeError(
                    f"Failed to insert new project records into database. {e}"
                ) from None

            # Insert new tasks for scraping project pages into database
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
                    "Failed to upsert new tasks for scraping "
                    f"project pages into database. {e}"
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
            self._db_client.mark_task_failure(task_id, str(e), retry_count)
            raise RuntimeError(str(e)) from None

        # Mark task as successful in database
        self._logger.info("Task succeeded")
        self._db_client.mark_task_success(task_id, retry_count)
