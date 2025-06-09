"""Data model for projects and their associations.
"""

from django.db import models


class Project(models.Model):
    """Metadata for a development project.
    """

    class Meta:
        db_table = "project"
        constraints = [
            models.UniqueConstraint(
                fields=["bank", "url"],
                name="unique_bank_url"
            )
        ]

    bank = models.ForeignKey(
        to="banks.Bank", 
		db_column="bank",
        on_delete=models.CASCADE
    )
    number = models.TextField(
		db_column="number",
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
    sector_list_raw = models.TextField(
		db_column="sector_list_raw",
		null=True
	)
    sector_list_stnd = models.TextField(
		db_column="sector_list_stnd",
		null=True
	)
    country_list_raw = models.TextField(
		db_column="country_list_raw",
		null=True
	)
    country_list_stnd = models.TextField(
		db_column="country_list_stnd",
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
    last_updated_utc = models.DateTimeField(
		db_column="last_updated_utc",
		auto_now_add=False,
		null=True
	)

class ProjectCountry(models.Model):
    """Links development projects to countries in a many-to-many relationship.
    """

    class Meta:
        db_table = "project_country"
        constraints = [
            models.UniqueConstraint(
                fields=["project", "country"],
                name="unique_project_country"
            )
        ]

    project = models.ForeignKey(
        to="projects.Project",
        db_column="project",
        on_delete=models.CASCADE
    )
    country = models.ForeignKey(
        to="countries.Country",
        db_column="country",
        on_delete=models.CASCADE
    )
    created_at_utc = models.DateTimeField(
        db_column="created_at_utc",
        auto_now_add=True,
        null=True
    )

class ProjectSector(models.Model):
    """An association between a project and a sector.
    """

    class Meta:
        db_table = "project_sector"
        constraints = [
            models.UniqueConstraint(
                fields=["project", "sector"],
                name="unique_project_sector"
            )
        ]

    project = models.ForeignKey(
        to="projects.Project", 
		db_column="project",
        on_delete=models.CASCADE
    )
    sector = models.ForeignKey(
        to="sectors.Sector", 
		db_column="sector",
        on_delete=models.CASCADE
    )
    created_at_utc = models.DateTimeField(
		db_column="created_at_utc",
		auto_now_add=True,
		null=True
	)
