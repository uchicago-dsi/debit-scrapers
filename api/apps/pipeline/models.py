"""Data models for pipeline entities.
"""

from django.db import models
from django.db.models.functions import Length

models.CharField.register_lookup(Length)


class Job(models.Model):
    """A pipeline job.
    """
    
    class Meta:
        db_table = "pipeline_job"
        constraints = [
            models.UniqueConstraint(
                fields=["invocation_id"],
                name="unique_invocation_id"
            )
        ]

    class JobChoices(models.TextChoices):
        """Types of pipeline jobs.
        """
        DEV_BANK_PROJECTS = "projects", "Development Bank Projects"
        FORM_13F = "form13f", "Form 13F"

    invocation_id = models.TextField(
		db_column="invocation_id",
		max_length=500
    )
    job_type = models.CharField(
		db_column="job_type",
		max_length=255,
		choices=JobChoices.choices,
		default="projects"
    )
    data_load_stage = models.CharField(
		db_column="data_load_stage",
		max_length=25,
		default="In Progress"
    )
    data_load_start_utc = models.DateTimeField(
		db_column="data_load_start_utc",
		auto_now_add=True,
		null=True
	)
    data_load_end_utc = models.DateTimeField(
		db_column="data_load_end_utc",
		null=True
	)
    data_clean_stage = models.CharField(
		db_column="data_clean_stage",
		max_length=25,
		default="Not Started"
    )
    data_clean_start_utc = models.DateTimeField(
		db_column="data_clean_start_utc",
		null=True
	)
    data_clean_end_utc = models.DateTimeField(
		db_column="data_clean_end_utc",
		null=True
	)   
    raw_url = models.URLField(
		db_column="raw_url",
		null=True
	)
    clean_url = models.URLField(
		db_column="clean_url",
		null=True
	)
    audit_url = models.TextField(
		db_column="audit_url",
		null=True
	)
    created_at_utc = models.DateTimeField(
		db_column="created_at_utc",
		auto_now_add=True,
		null=True
	)

class Task(models.Model):
    """A task for a pipeline job.
    """

    class Meta:
        db_table = "pipeline_task"
        constraints = [
            models.UniqueConstraint(
                fields=["job", "source", "workflow_type", "url"],
                name="unique_source_url"
            )
        ]

    job = models.ForeignKey(
        to="pipeline.Job", 
		db_column="job",
        on_delete=models.CASCADE
    )
    status = models.CharField(
		db_column="status",
		max_length=100
    )
    source = models.CharField(
		db_column="source",
		max_length=100
    )
    url = models.TextField(
		db_column="url",
		null=True
	)
    workflow_type = models.CharField(
		db_column="workflow_type",
		max_length=100
    )
    processing_start_utc = models.DateTimeField(
		db_column="processing_start_utc",
		null=True
	)
    processing_end_utc = models.DateTimeField(
		db_column="processing_end_utc",
		null=True
	)
    scraping_start_utc = models.DateTimeField(
		db_column="scraping_start_utc",
		null=True
	)
    scraping_end_utc = models.DateTimeField(
		db_column="scraping_end_utc",
		null=True
	)
    last_failed_at_utc = models.DateTimeField(
		db_column="last_failed_at_utc",
		null=True
	)
    last_error_message = models.TextField(
		db_column="last_error_message",
		null=True
	)
    retry_count = models.SmallIntegerField(
		db_column="retry_count",
		default=0
    )
    created_at_utc = models.DateTimeField(
		db_column="created_at_utc",
		auto_now_add=True,
		null=True
	)

class StagedEquityInvestment(models.Model):
    """Data model for staged institutional equity
    investments reported in the U.S. Security and
    Exchange Commission"s Form 13F.
    """

    class Meta:
        db_table = "form_13f_staged_investment"
        constraints = [
            models.CheckConstraint(
                check=models.Q(company_cik__length=10),
                name="staged_cik_standard_length"
            ),
            models.UniqueConstraint(
                fields=[
                    "form_url",
                    "stock_cusip",
                    "stock_investment_discretion",
                    "stock_manager"
                ], 
                name="unique_investment_url"
            )
        ]

    task = models.ForeignKey(
        to="pipeline.Task", 
		db_column="task",
        on_delete=models.CASCADE
    )
    company_cik = models.CharField(
		db_column="company_cik",
		max_length=10
    )
    company_name = models.TextField(
		db_column="company_name"
    )
    form_name = models.TextField(
		db_column="form_name"
    )
    form_accession_number = models.TextField(
		db_column="form_accession_number"
    )
    form_report_period = models.DateField(
		db_column="form_report_period"
    )
    form_filing_date = models.DateField(
		db_column="form_filing_date",
		null=True
	)
    form_acceptance_date = models.DateField(
		db_column="form_acceptance_date",
		null=True
	)
    form_effective_date = models.DateField(
		db_column="form_effective_date",
		null=True
	)
    form_url = models.URLField(
		db_column="form_url"
    )
    stock_issuer_name = models.TextField(
		db_column="stock_issuer_name"
    )
    stock_title_class = models.TextField(
		db_column="stock_title_class"
    )
    stock_cusip = models.CharField(
		db_column="stock_cusip",
		max_length=9
    )
    stock_value_x1000 = models.IntegerField(
		db_column="stock_value_x1000"
    )
    stock_shares_prn_amt = models.IntegerField(
		db_column="stock_shares_prn_amt"
    )
    stock_sh_prn = models.CharField(
		db_column="stock_sh_prn",
		max_length=255
    )
    stock_put_call = models.TextField(
		db_column="stock_put_call",
		null=True
	)
    stock_investment_discretion = models.CharField(
		db_column="stock_investment_discretion",
		max_length=255
    )
    stock_manager = models.TextField(
		db_column="stock_manager",
        blank=True,
		default=""
    )
    stock_voting_auth_sole = models.IntegerField(
		db_column="stock_voting_auth_sole",
		null=True
	)
    stock_voting_auth_shared = models.IntegerField(
		db_column="stock_voting_auth_shared",
		null=True
	)
    stock_voting_auth_none = models.IntegerField(
		db_column="stock_voting_auth_none",
		null=True
	)
    created_at_utc = models.DateTimeField(
		db_column="created_at_utc",
		auto_now_add=True,
		null=True
	)

class StagedProject(models.Model):
    """Data model for staged projects--i.e., raw
    development bank project records retrieved by 
    bulk download, web scraping, or API querying.
    """

    class Meta:
        db_table = "staged_project"
        constraints = [
            models.UniqueConstraint(
                fields=["bank", "url"],
                name="unique_staged_project"
            )
        ]

    task = models.ForeignKey("pipeline.Task", 
		db_column="task", 
        on_delete=models.CASCADE
    )
    bank = models.CharField(
		db_column="bank",
		max_length=25
    )
    number = models.CharField(
		db_column="number",
		max_length=255,
		null=True
	)
    name = models.TextField(
		db_column="name",
		null=True
	)
    status = models.CharField(
		db_column="status",
		max_length=50,
		null=True
	)
    year = models.SmallIntegerField(
		db_column="year",
		null=True
	)
    month = models.SmallIntegerField(
		db_column="month",
		null=True
	)
    day = models.SmallIntegerField(
		db_column="day",
		null=True
	)
    loan_amount = models.FloatField(
		db_column="loan_amount",
		null=True
	)
    loan_amount_currency = models.CharField(
		db_column="loan_amount_currency",
		max_length=50,
		null=True
	)
    loan_amount_in_usd = models.FloatField(
		db_column="loan_amount_in_usd",
		null=True
	)
    sectors = models.TextField(
		db_column="sectors",
		null=True
	)
    countries = models.TextField(
		db_column="countries",
		null=True
	)
    companies = models.TextField(
		db_column="companies",
		null=True
	)
    url = models.TextField(
		db_column="url",
		null=True
	)
    created_at_utc = models.DateTimeField(
		db_column="created_at_utc",
		auto_now_add=True,
		null=True
	)
