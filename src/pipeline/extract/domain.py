"""Domain entities used throughout the application."""

# Standard library imports
from datetime import datetime
from typing import TypedDict


class JobUpdateRequest(TypedDict):
    """Represents a request to update an extration job record."""

    invocation_id: str | None = None
    """The unique identifier for the processing job encapsulating 
    all development bank data scraping and cleaning tasks."""

    started_at_utc: datetime | None = None
    """The UTC timestamp indicating when the job started processing."""

    completed_at_utc: datetime | None = None
    """The UTC timestamp indicating when the job successfully completed."""

    results_storage_key: str | None = None
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

    id: int | None = None
    """The unique identifier for the task."""

    status: str | None = None
    """The processing status of the task (e.g., "Completed", "Error")."""

    started_at_utc: datetime | None = None
    """The UTC timestamp indicating when the task started processing."""

    completed_at_utc: datetime | None = None
    """The UTC timestamp indicating when the task successfully completed."""

    failed_at_utc: datetime | None = None
    """The UTC timestamp indicating when the task last failed."""

    last_error: str | None = None
    """The last exception message generated, if any."""

    retry_count: int | None = None
    """The number of times the message has been reprocessed."""


class StagedProjectUpsertRequest(TypedDict):
    """Represents a request to upsert astaged project record."""

    affiliates: str | None
    "The organizations affiliated with the project. Pipe-delimited."

    countries: str | None
    "The countries in which the project is located. Pipe-delimited."

    date_actual_close: str | None
    "The actual end date for project funding. Formatted as YYYY, YYYY-MM, or YYYY-MM-DD."

    date_approved: str | None
    """The date the project funding was approved by the bank. Formatted as YYYY, YYYY-MM, or YYYY-MM-DD."""

    date_disclosed: str | None
    """The date the project was disclosed to the public. Formatted as YYYY, YYYY-MM, or YYYY-MM-DD."""

    date_effective: str | None
    "The date the project funding became effective. Formatted as YYYY, YYYY-MM, or YYYY-MM-DD."

    date_last_updated: str | None
    "The date the project details were last updated. Formatted as YYYY, YYYY-MM, or YYYY-MM-DD."

    date_submitted: str | None
    "The original projected end date for project funding. Formatted as YYYY, YYYY-MM, or YYYY-MM-DD."

    date_planned_close: str | None
    """The date the project was last updated. Formatted as YYYY, YYYY-MM, or YYYY-MM-DD."""

    date_planned_effective: str | None
    "The estimated start date of the project funding. Formatted as YYYY, YYYY-MM, or YYYY-MM-DD."

    date_revised_close: str | None
    """The revised end date for project funding. Formatted as YYYY, YYYY-MM, or YYYY-MM-DD."""

    date_signed: str | None
    "The date the project contract was signed by the bank. Formatted as YYYY, YYYY-MM, or YYYY-MM-DD."

    date_under_appraisal: str | None
    """The date the project came under appraisal by the bank. Formatted as YYYY, YYYY-MM, or YYYY-MM-DD."""

    finance_types: str | None
    """The funding types used for the project. Pipe-delimited."""

    name: str | None
    """The project name."""

    number: str | None
    """The project number."""

    sectors: str | None
    """The economic sectors impacted by the project. Pipe-delimited."""

    source: str
    """The name of the development bank."""

    status: str | None
    """The project status."""

    total_amount: float | None
    """The project debt amount."""

    total_amount_currency: str | None
    """The project debt amount currency."""

    total_amount_usd: float | None
    """The project debt amount in USD."""

    url: str
    """The project URL."""
