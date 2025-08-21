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

    affiliates: Optional[str]
    "The organizations affiliated with the project. Pipe-delimited."

    countries: Optional[str]
    "The countries in which the project is located. Pipe-delimited."

    date_actual_close: Optional[str]
    "The actual end date for project funding. Formatted as YYYY, YYYY-MM, or YYYY-MM-DD."

    date_approved: Optional[str]
    """The date the project funding was approved by the bank. Formatted as YYYY, YYYY-MM, or YYYY-MM-DD."""

    date_disclosed: Optional[str]
    """The date the project was disclosed to the public. Formatted as YYYY, YYYY-MM, or YYYY-MM-DD."""

    date_effective: Optional[str]
    "The date the project funding became effective. Formatted as YYYY, YYYY-MM, or YYYY-MM-DD."

    date_last_updated: Optional[str]
    "The date the project details were last updated. Formatted as YYYY, YYYY-MM, or YYYY-MM-DD."

    date_submitted: Optional[str]
    "The original projected end date for project funding. Formatted as YYYY, YYYY-MM, or YYYY-MM-DD."

    date_planned_close: Optional[str]
    """The date the project was last updated. Formatted as YYYY, YYYY-MM, or YYYY-MM-DD."""

    date_planned_effective: Optional[str]
    "The estimated start date of the project funding. Formatted as YYYY, YYYY-MM, or YYYY-MM-DD."

    date_revised_close: Optional[str]
    """The revised end date for project funding. Formatted as YYYY, YYYY-MM, or YYYY-MM-DD."""

    date_signed: Optional[str]
    "The date the project contract was signed by the bank. Formatted as YYYY, YYYY-MM, or YYYY-MM-DD."

    date_under_appraisal: Optional[str]
    """The date the project came under appraisal by the bank. Formatted as YYYY, YYYY-MM, or YYYY-MM-DD."""

    finance_types: Optional[str]
    """The funding types used for the project. Pipe-delimited."""

    name: Optional[str]
    """The project name."""

    number: Optional[str]
    """The project number."""

    sectors: Optional[str]
    """The economic sectors impacted by the project. Pipe-delimited."""

    source: str
    """The name of the development bank."""

    status: Optional[str]
    """The project status."""

    total_amount: Optional[float]
    """The project debt amount."""

    total_amount_currency: Optional[str]
    """The project debt amount currency."""

    total_amount_usd: Optional[float]
    """The project debt amount in USD."""

    url: str
    """The project URL."""
