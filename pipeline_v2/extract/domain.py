"""Domain entities used throughout the application."""

# Standard library imports
from datetime import datetime
from typing import Optional, TypedDict


class JobUpdateRequest(TypedDict):
    """Represents a request to update an extration job record."""

    invocation_id: Optional[str] = None
    """The unique identifier for the processing job encapsulating 
    all development bank data scraping and cleaning tasks."""

    started_at_utc: Optional[datetime] = None
    """The UTC timestamp indicating when the job started processing."""

    completed_at_utc: Optional[datetime] = None
    """The UTC timestamp indicating when the job successfully completed."""

    results_storage_key: Optional[str] = None
    """The storage key where the results are stored."""


class TaskInsertRequest(TypedDict):
    """Represents a request to create a new workflow task."""

    job_id: int
    """The unique identifier for the processing job encapsulating 
    all development bank data scraping and cleaning tasks."""

    status: str
    """The initial/default status for the task (e.g., "Not Started")."""

    source: str
    """The name of the data source."""

    url: str
    """The URL of the data source to scrape or query."""

    workflow_type: str
    """The type of workflow necessary to complete the task."""


class TaskUpdateRequest(TypedDict):
    """Represents a request to update a workflow task."""

    id: Optional[int] = None
    """The unique identifier for the task."""

    status: Optional[str] = None
    """The processing status of the task (e.g., "Completed", "Error")."""

    started_at_utc: Optional[datetime] = None
    """The UTC timestamp indicating when the task started processing."""

    completed_at_utc: Optional[datetime] = None
    """The UTC timestamp indicating when the task successfully completed."""

    failed_at_utc: Optional[datetime] = None
    """The UTC timestamp indicating when the task last failed."""

    last_error: Optional[str] = None
    """The last exception message generated, if any."""

    retry_count: Optional[int] = None
    """The number of times the message has been reprocessed."""


class StagedProjectUpsertRequest(TypedDict):
    """Represents a request to upsert astaged project record."""

    bank: str
    """The name of the development bank."""

    number: Optional[str]
    """The project number."""

    name: Optional[str]
    """The project name."""

    status: Optional[str]
    """The project status."""

    year: Optional[int]
    """The project year."""

    month: Optional[int]
    """The project month."""

    day: Optional[int]
    """The project day."""

    loan_amount: Optional[float]
    """The project loan amount."""

    loan_amount_in_usd: Optional[float]
    """The project loan amount in USD."""

    currency: Optional[str]
    """The project loan amount currency."""

    sectors: Optional[str]
    """The project sectors."""

    countries: Optional[str]
    """The project countries."""

    companies: Optional[str]
    """The project companies."""

    url: str
    """The project URL."""
