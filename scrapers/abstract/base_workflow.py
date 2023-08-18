"""Functionality common to all data collection
workflows for development bank projects.
"""

from abc import ABC, abstractmethod, abstractproperty
from logging import Logger
from typing import Optional


class BaseWorkflow(ABC):
    """An abstract class representing a generic
    data collection workflow for development
    bank projects.
    """

    def __init__(self, logger: Logger) -> None:
        """Initializes a new instance of a `BaseWorkflow`.

        Args:
            logger (`Logger`): An instance of the logging class.

        Returns:
            None
        """
        self._logger = logger


    @abstractproperty
    def next_workflow(self) -> str:
        """The name of the workflow to execute, if any.
        """
        raise NotImplementedError


    @abstractmethod
    def execute(
        self,
        message_id: str,
        num_delivery_attempts: int,
        job_id: str,
        task_id: str,
        source: str,
        url: Optional[str]=None) -> None:
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
        raise NotImplementedError
