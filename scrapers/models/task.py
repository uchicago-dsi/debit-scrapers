"""Models used throughout the application.
"""

from datetime import datetime


class TaskRequest:
    """Represents a request to create a new workflow task.
    """

    def __init__(
        self,
        job_id: int,
        status: str,
        bank: str,
        url: str,
        workflow_type: str) -> None:
        """Initializes an instance of a new `TaskRequest`.

        Args:
            job_id (int): The unique identifier for the processing
                job encapsulating all development bank data scraping
                and cleaning tasks.

            status (`str`): The initial/default status for the task
                (e.g., "Not Started").

            bank (`str`): The name of the development bank to scrape.

            url (list of str): The URL of the page to scrape.

            workflow_type (`str`): The type of workflow necessary to
                complete the task (e.g., "Project Page Scrape",
                "Results Page Scrape").

        Returns:
            None
        """
        self.job_id = job_id
        self.status = status
        self.bank = bank
        self.url = url
        self.workflow_type = workflow_type


class TaskUpdate:
    """Represents an update for a workflow task.
    """
    
    def __init__(self) -> None:
        """Initializes a new instance of a `TaskUpdate`.
        
        Args:

            id (int): The unique identifier for the task.

            status (`str`): The processing status of the task
                (e.g., "Completed", "Error").

            processing_start_utc (datetime): The UTC timestamp 
                indicating when processing of the message started.

            processing_end_utc (datetime or None): The UTC timestamp
                indicating when processing of the message 
                successfully completed.

            scraping_start_utc (datetime or None): The UTC timestamp
                indicating when page scraping started.

            scraping_end_utc (datetime or None): The UTC timestamp
                indicating when page scraping successfully ended.

            last_failed_at_utc (datetime or None): The UTC timestamp
                indicating when the last workflow failure occurred, if any.

            last_error_message (`str`): The last exception message
                generated, if any.

            retry_count (int): The number of times the 
                message has been reprocessed.

        Args:
            task_id (`str`): The unique identifier for the task.

        Returns:
            None
        """
        self.id: int = None
        self.status: str = None
        self.processing_start_utc: datetime = None
        self.processing_end_utc: datetime = None
        self.scraping_start_utc: datetime = None
        self.scraping_end_utc: datetime = None
        self.last_failed_at_utc: datetime = None
        self.last_error_message: str = None
        self.retry_count: int = None
