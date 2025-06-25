"""Models used throughout the application."""

# Third-party imports
from django.db import models


class ExtractionJob(models.Model):
    """A job to extract development bank project
    records from the configured data sources.
    Triggered by an external service.
    """

    class Meta:
        db_table = "extraction_job"
        constraints = [
            models.UniqueConstraint(
                fields=["invocation_id"], name="unique_invocation_id"
            )
        ]

    invocation_id = models.CharField(max_length=500)
    """The unique identifier assigned to the job by the external trigger."""

    started_at_utc = models.DateTimeField(auto_now_add=True, null=True)
    """The start time of the job in UTC."""

    completed_at_utc = models.DateTimeField(null=True)
    """The end time of the job in UTC."""

    result_storage_key = models.TextField()
    """The result file location in the configured Cloud Storage bucket."""


class ExtractionTask(models.Model):
    """A task to extract data from a single endpoint (e.g.,
    a webpage URL or API call) as part of a larger job.
    """

    class Meta:
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
        ERROR = "Error"

    job = models.ForeignKey("extract.ExtractionJob", on_delete=models.CASCADE)
    """The parent job."""

    status = models.CharField(
        max_length=255, choices=StatusChoices.choices, default=StatusChoices.NOT_STARTED
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
    """Data model for extracted projects--i.e., raw
    development bank project records retrieved by
    bulk download, web scraping, or API querying.
    """

    class Meta:
        db_table = "extracted_project"
        constraints = [
            models.UniqueConstraint(
                fields=["bank", "url"], name="unique_extracted_project"
            )
        ]

    task = models.ForeignKey("extract.ExtractionTask", on_delete=models.CASCADE)
    """The parent task."""

    bank = models.CharField(max_length=255)
    """The name of the development bank or financial institution."""

    number = models.CharField(max_length=255, null=True)
    """The unique identifier assigned to the project by the bank."""

    name = models.TextField(blank=True, default="")
    """The project name."""

    status = models.CharField(max_length=50, null=True)
    """The current project status."""

    year = models.SmallIntegerField(null=True)
    """ The year the project was approved."""

    month = models.SmallIntegerField(null=True)
    """The month the project was approved."""

    day = models.SmallIntegerField(null=True)
    """The day the project was approved."""

    loan_amount = models.FloatField(null=True)
    """The current loan amount."""

    loan_amount_in_usd = models.FloatField(null=True)
    """The current loan amount in USD, if provided."""

    currency = models.CharField(max_length=50, null=True)
    """The currency of the loan."""

    sectors = models.TextField(null=True)
    """The sectors impacted by the project."""

    countries = models.TextField(null=True)
    """The countries in which the project is located."""

    companies = models.TextField(null=True)
    """The companies involved in the project."""

    created_at_utc = models.DateTimeField(auto_now_add=True, null=True)
    """The time at which the record was created in UTC."""

    last_updated_at_utc = models.DateTimeField(auto_now=True, null=True)
    """The time at which the record was last updated in UTC."""

    url = models.URLField(db_column="url", null=True)
    """The URL to the project page on the bank's website."""
