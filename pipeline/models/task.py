"""Models used throughout the application.
"""

from dataclasses import dataclass
from datetime import datetime
from typing import List, Optional


@dataclass
class TaskRequest:
    """Represents a request to create a new workflow task.
    """

    job_id: int
    """The unique identifier for the processing job encapsulating 
    all development bank data scraping and cleaning tasks."""

    status: str
    """The initial/default status for the task (e.g., "Not Started")."""

    bank: str
    """The name of the development bank to scrape."""

    url: List[str]
    """The URL of the page to scrape."""

    workflow_type: str
    """The type of workflow necessary to complete the task 
    (e.g., "Project Page Scrape", "Results Page Scrape")."""

@dataclass
class TaskUpdate:
    """Represents an update for a workflow task.
    """

    id: Optional[int] = None
    """The unique identifier for the task."""

    status: Optional[str] = None
    """The processing status of the task (e.g., "Completed", "Error")."""

    processing_start_utc: Optional[datetime] = None
    """The UTC timestamp indicating when processing of the message started."""
    
    processing_end_utc: Optional[datetime] = None
    """The UTC timestamp indicating when processing of the message 
    successfully completed."""

    scraping_start_utc: Optional[datetime] = None
    """The UTC timestamp indicating when page scraping started."""

    scraping_end_utc: Optional[datetime] = None
    """The UTC timestamp indicating when page scraping successfully ended."""

    last_failed_at_utc: Optional[datetime] = None
    """The UTC timestamp indicating when the last workflow 
    failure occurred, if any."""

    last_error_message: Optional[str] = None
    """The last exception message generated, if any."""

    retry_count: Optional[int] = None
    """The number of times the message has been reprocessed."""
