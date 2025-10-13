"""Domain entities used throughout the application."""

# Standard library imports
from dataclasses import dataclass
from datetime import datetime
from typing import TypedDict


class TaskUpsertRequest(TypedDict):
    """Represents a request to upsert a workflow task."""

    job_id: int
    """The unique identifier for the parent job."""

    source: str
    """The name of the data source."""

    workflow_type: str
    """The type of workflow necessary to complete the task."""

    url: str
    """The URL of the data source to scrape or query."""


@dataclass
class ProjectUpsertRequest:
    """Represents a request to upsert astaged project record."""

    task_id: int
    """The unique identifier for the parent task."""

    source: str
    """The name of the development bank."""

    url: str
    """The project URL."""

    affiliates: str = ""
    "The organizations affiliated with the project. Pipe-delimited."

    countries: str = ""
    "The countries in which the project is located. Pipe-delimited."

    date_actual_close: str = ""
    "The actual end date for project funding. Formatted as YYYY, YYYY-MM, or YYYY-MM-DD."

    date_approved: str = ""
    """The date the project funding was approved by the bank. Formatted as YYYY, YYYY-MM, or YYYY-MM-DD."""

    date_disclosed: str = ""
    """The date the project was disclosed to the public. Formatted as YYYY, YYYY-MM, or YYYY-MM-DD."""

    date_effective: str = ""
    "The date the project funding became effective. Formatted as YYYY, YYYY-MM, or YYYY-MM-DD."

    date_last_updated: str = ""
    "The date the project details were last updated. Formatted as YYYY, YYYY-MM, or YYYY-MM-DD."

    date_planned_close: str = ""
    """The date the project was last updated. Formatted as YYYY, YYYY-MM, or YYYY-MM-DD."""

    date_planned_effective: str = ""
    "The estimated start date of the project funding. Formatted as YYYY, YYYY-MM, or YYYY-MM-DD."

    date_revised_close: str = ""
    """The revised end date for project funding. Formatted as YYYY, YYYY-MM, or YYYY-MM-DD."""

    date_signed: str = ""
    "The date the project contract was signed by the bank. Formatted as YYYY, YYYY-MM, or YYYY-MM-DD."

    date_under_appraisal: str = ""
    """The date the project came under appraisal by the bank. Formatted as YYYY, YYYY-MM, or YYYY-MM-DD."""

    finance_types: str = ""
    """The funding types used for the project. Pipe-delimited."""

    fiscal_year_effective: str = ""
    """The fiscal year in which the project funding became effective. Formatted as YYYY."""

    name: str = ""
    """The project name."""

    number: str = ""
    """The project number."""

    sectors: str = ""
    """The economic sectors impacted by the project. Pipe-delimited."""

    status: str = ""
    """The project status."""

    total_amount: float | None = None
    """The project debt amount."""

    total_amount_currency: str = ""
    """The project debt amount currency."""

    total_amount_usd: float | None = None
    """The project debt amount in USD."""

    created_at_utc: datetime | None = None
    """The UTC timestamp indicating when the project record was created."""

    last_updated_at_utc: datetime | None = None
    """The UTC timestamp indicating when the project record was last updated."""
