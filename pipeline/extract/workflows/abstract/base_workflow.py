"""Functionality common to all extraction workflows."""

# Standard library imports
from abc import ABC, abstractmethod
from logging import Logger


class BaseWorkflow(ABC):
    """An abstract class representing a generic
    data collection workflow for web scraping.
    """

    def __init__(self, logger: Logger) -> None:
        """Initializes a new instance of a `BaseWorkflow`.

        Args:
            logger: A standard logger instance.

        Returns:
            `None`
        """
        self._logger = logger

    @property
    @abstractmethod
    def next_workflow(self) -> str:
        """The name of the workflow to execute, if any."""
        raise NotImplementedError

    @abstractmethod
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
        raise NotImplementedError
