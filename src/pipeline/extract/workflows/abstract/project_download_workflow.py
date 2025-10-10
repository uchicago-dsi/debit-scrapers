"""Project Download Workflow

Defines the steps necessary to download project data
for a development bank. Implements the template design
pattern to allow customization by subclasses.
"""

# Standard library imports
import json
from abc import abstractmethod
from logging import Logger

# Third-party imports
import pandas as pd

# Application imports
from common.http import DataRequestClient
from extract.dal import DatabaseClient
from extract.workflows.abstract import BaseWorkflow


class ProjectDownloadWorkflow(BaseWorkflow):
    """Base class for all project download workflows.

    Defines a template pattern in which project records
    are directly downloaded from a development bank website
    or queried from an API and then cleaned and saved to a
    database. Handles data persistence and status updates in
    the database. Subclasses implement logic specific to
    the data source.

    This workflow differs from `ProjectScrapeWorkflow` in that
    all of its project records are located at one persistent
    URL/resource.
    """

    def __init__(
        self,
        data_request_client: DataRequestClient,
        db_client: DatabaseClient,
        logger: Logger,
    ) -> None:
        """Initializes a new instance of a `ProjectDownloadWorkflow`.

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
    def get_projects(self) -> pd.DataFrame:
        """Fetches project records and reads them into a Pandas DataFrame.

        Args:
            `None`

        Returns:
            The raw project records.
        """
        raise NotImplementedError

    @abstractmethod
    def clean_projects(self, df: pd.DataFrame) -> pd.DataFrame:
        """Cleans project records to conform to an expected schema.

        Args:
            df: The raw project records.

        Returns:
            The cleaned records.
        """
        raise NotImplementedError

    def execute(
        self,
        message_id: str,
        num_delivery_attempts: int,
        job_id: str,
        task_id: str,
        source: str,
        url: str = None,
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
            clean_project_df = self.clean_projects(raw_project_df)

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
