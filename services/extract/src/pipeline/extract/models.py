"""Models used throughout the application."""

# Third-party imports
from django.db import models


class ExtractionJob(models.Model):
    """Database model for data extraction jobs."""

    class Meta:
        """Metadata for the model."""

        db_table = "extraction_job"
        constraints = [models.UniqueConstraint(fields=["date"], name="unique_date")]

    date = models.CharField(max_length=10)
    """The unique identifier assigned to the job."""

    started_at_utc = models.DateTimeField(auto_now_add=True, null=True)
    """The start time of the job in UTC."""

    completed_at_utc = models.DateTimeField(null=True)
    """The end time of the job in UTC."""


class ExtractionTask(models.Model):
    """Database model for data extraction tasks within a parent job.

    A task is scoped to a single URL (e.g., a webpage or an API ejdpoint).
    """

    class Meta:
        """Metadata for the model."""

        db_table = "extraction_task"
        constraints = [
            models.UniqueConstraint(
                fields=["job", "source", "workflow_type", "url"],
                name="unique_source_url",
            )
        ]

    class StatusChoices(models.TextChoices):
        """Enumerates possible task statuses."""

        NOT_STARTED = "Not Started"
        IN_PROGRESS = "In Progress"
        COMPLETED = "Completed"
        CANCELLED = "Cancelled"
        ERROR = "Error"

    job = models.ForeignKey("extract.ExtractionJob", on_delete=models.CASCADE)
    """The parent job."""

    status = models.CharField(
        max_length=255,
        choices=StatusChoices.choices,
        default=StatusChoices.NOT_STARTED,
    )
    """The status of the task."""

    source = models.CharField(max_length=255)
    """The source of the task."""

    workflow_type = models.CharField(max_length=255)
    """The workflow type of the task."""

    created_at_utc = models.DateTimeField(auto_now_add=True, null=True)
    """The creation time of the task in UTC."""

    started_at_utc = models.DateTimeField(null=True)
    """The start time of the task in UTC."""

    completed_at_utc = models.DateTimeField(null=True)
    """The end time of the task in UTC."""

    failed_at_utc = models.DateTimeField(null=True)
    """The last failed time of the task in UTC."""

    last_error = models.TextField(blank=True, default="")
    """The last error message for the task."""

    retry_count = models.SmallIntegerField(default=0)
    """The number of times the task has been retried."""

    url = models.TextField(blank=True, default="")
    """The URL of the webpage to scrape or process."""


class ExtractedProject(models.Model):
    """Database model for an extracted development project."""

    class Meta:
        """Metadata for the model."""

        db_table = "extracted_project"
        constraints = [
            models.UniqueConstraint(
                fields=["source", "url"], name="unique_extracted_project"
            )
        ]

    created_at_utc = models.DateTimeField(auto_now_add=True)
    """The time at which the record was created in UTC."""

    last_updated_at_utc = models.DateTimeField(auto_now=True)
    """The time at which the record was last updated in UTC."""

    task = models.ForeignKey("extract.ExtractionTask", on_delete=models.CASCADE)
    """The parent task."""

    affiliates = models.TextField(blank=True, default="")
    """The organizations involved in the project. Pipe-delimited."""

    countries = models.TextField(blank=True, default="")
    """The countries in which the project is located. Pipe-delimited."""

    date_actual_close = models.TextField(blank=True, default="")
    """The actual end date for project funding.
    Formatted as YYYY, YYYY-MM, or YYYY-MM-DD.
    """

    date_approved = models.TextField(blank=True, default="")
    """The date the project funding was approved by the bank.
    Formatted as YYYY, YYYY-MM, or YYYY-MM-DD.
    """

    date_disclosed = models.TextField(blank=True, default="")
    """The date the project was disclosed to the public.
    Formatted as YYYY, YYYY-MM, or YYYY-MM-DD.
    """

    date_effective = models.TextField(blank=True, default="")
    """The date the project funding became effective.
    Formatted as YYYY, YYYY-MM, or YYYY-MM-DD.
    """

    date_last_updated = models.TextField(blank=True, default="")
    """The date the project details were last updated for the public.
    Formatted as YYYY, YYYY-MM, or YYYY-MM-DD.
    """

    date_planned_close = models.TextField(blank=True, default="")
    """The original projected end date for project funding.
    Formatted as YYYY, YYYY-MM, or YYYY-MM-DD.
    """

    date_planned_effective = models.TextField(blank=True, default="")
    """The estimated start date for project funding.
    Formatted as YYYY, YYYY-MM, or YYYY-MM-DD.
    """

    date_revised_close = models.TextField(blank=True, default="")
    """The revised end date for project funding.
    Formatted as YYYY, YYYY-MM, or YYYY-MM-DD.
    """

    date_signed = models.TextField(blank=True, default="")
    """The date the project contract was signed by the bank.
    Formatted as YYYY, YYYY-MM, or YYYY-MM-DD.
    """

    date_under_appraisal = models.TextField(blank=True, default="")
    """The date the project funding came under appraisal by the bank.
    Formatted as YYYY, YYYY-MM, or YYYY-MM-DD.
    """

    finance_types = models.TextField(blank=True, default="")
    """The funding types used for the project. Pipe-delimited."""

    fiscal_year_effective = models.TextField(blank=True, default="")
    """The fiscal year in which the project funding became effective.
    Formatted as YYYY.
    """

    name = models.TextField(blank=True, default="")
    """The project name, if any."""

    number = models.CharField(max_length=255, blank=True, default="")
    """The unique identifier assigned to the project by the bank, if any."""

    sectors = models.TextField(blank=True, default="")
    """The economic sectors impacted by the project. Pipe-delimited."""

    source = models.CharField(max_length=255)
    """The abbreviation of the parent data source."""

    status = models.CharField(max_length=50, blank=True, default="")
    """The current project status."""

    total_amount = models.DecimalField(null=True, decimal_places=2, max_digits=20)
    """The total amount of funding awarded to the project."""

    total_amount_currency = models.CharField(max_length=50, blank=True, default="")
    """The currency of the loan."""

    total_amount_usd = models.DecimalField(null=True, decimal_places=2, max_digits=20)
    """The current loan amount in USD, if provided."""

    url = models.URLField(blank=True, default="", max_length=2000)
    """The URL to the project page on the bank's website."""
