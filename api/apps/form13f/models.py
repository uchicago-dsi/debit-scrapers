"""Data models for entities associated with Form 13F submissions
to the U.S. Securities and Exchange Commission (S.E.C.).
"""

from django.db import models


class Form13FCompany(models.Model):
    """Represents a company reporting its quarterly investments
    through Form 13F. NOTE: Names may change over time.
    """

    class Meta:
        db_table = "form_13f_company"
        constraints = [
            models.CheckConstraint(
                check=models.Q(cik__length=10), 
                name="company_cik_standard_length"
            ),
            models.UniqueConstraint(
                fields=["cik", "name"], 
                name="unique_company_cik"
            ),
        ]
        indexes = [models.Index(fields=["cik"]), models.Index(fields=["name"])]

    cik = models.CharField(db_column="cik", max_length=10)
    name = models.TextField(db_column="name")
    created_at_utc = models.DateTimeField(
        db_column="created_at_utc", auto_now_add=True, null=True
    )


class Form13FSubmission(models.Model):
    """Represents a single Form 13F submission."""

    class Meta:
        db_table = "form_13f_submission"
        constraints = [
            models.UniqueConstraint(
                fields=["accession_number"],
                name="unique_form_accession_number"
            )
        ]
        indexes = [
            models.Index(fields=["accession_number"]),
            models.Index(fields=["report_period"]),
            models.Index(fields=["filing_date"]),
            models.Index(fields=["acceptance_date"]),
            models.Index(fields=["effective_date"]),
        ]

    company = models.ForeignKey(
        to="form13f.Form13FCompany", 
        db_column="company", 
        on_delete=models.CASCADE
    )
    name = models.TextField(db_column="name")
    accession_number = models.TextField(db_column="accession_number")
    report_period = models.DateField(db_column="report_period")
    filing_date = models.DateField(db_column="filing_date", null=True)
    acceptance_date = models.DateField(db_column="acceptance_date", null=True)
    effective_date = models.DateField(db_column="effective_date", null=True)
    url = models.URLField(db_column="url")
    created_at_utc = models.DateTimeField(
        db_column="created_at_utc",
        auto_now_add=True, 
        null=True
    )


class Form13FInvestment(models.Model):
    """Represents an investment listed in a Form 13F submission."""

    class Meta:
        db_table = "form_13f_investment"
        constraints = [
            models.UniqueConstraint(
                fields=["form", "cusip", "investment_discretion", "manager"],
                name="unique_form13f_investment",
            )
        ]
        indexes = [
            models.Index(fields=["issuer_name"]),
            models.Index(fields=["exchange_code"]),
            models.Index(fields=["cusip"]),
            models.Index(fields=["title_class"]),
            models.Index(fields=["market_sector"]),
            models.Index(fields=["security_type"]),
            models.Index(fields=["ticker"]),
            models.Index(fields=["investment_discretion"]),
        ]

    form = models.ForeignKey(
        to="form13f.Form13FSubmission",
        db_column="form",
        on_delete=models.CASCADE
    )
    exchange_code = models.TextField(db_column="exchange_code", null=True)
    issuer_name = models.TextField(db_column="issuer_name")
    cusip = models.CharField(db_column="cusip", max_length=9)
    title_class = models.TextField(db_column="title_class", null=True)
    market_sector = models.CharField(db_column="market_sector", max_length=255)
    security_type = models.CharField(db_column="security_type", max_length=255)
    ticker = models.CharField(db_column="ticker", max_length=255)
    value_x1000 = models.IntegerField(db_column="value_x1000")
    shares_prn_amt = models.IntegerField(db_column="shares_prn_amt")
    sh_prn = models.CharField(db_column="sh_prn", max_length=255)
    put_call = models.TextField(db_column="put_call", null=True)
    investment_discretion = models.CharField(
        db_column="investment_discretion", max_length=255
    )
    manager = models.TextField(db_column="manager", blank=True, default="")
    voting_auth_sole = models.IntegerField(
        db_column="voting_auth_sole", 
        null=True
      )
    voting_auth_shared = models.IntegerField(
        db_column="voting_auth_shared", 
        null=True
      )
    voting_auth_none = models.IntegerField(
        db_column="voting_auth_none", 
        null=True
      )
    created_at_utc = models.DateTimeField(
        db_column="created_at_utc", 
        auto_now_add=True, 
        null=True
    )


class LatestForm13FInvestment(models.Model):
    """Represents an investment from a Form 13F
    submitted in the latest financial quarter.
    """

    class Meta:
        managed = False
        db_table = "form_13f_investment_latest"

    company = models.ForeignKey(
        to="form13f.Form13FCompany",
        db_column="company",
        on_delete=models.CASCADE
    )
    company_cik = models.CharField(db_column="company_cik", max_length=10)
    company_name = models.TextField(db_column="company_name")
    form = models.ForeignKey(
        to="form13f.Form13FSubmission", 
        db_column="form", 
        on_delete=models.CASCADE
    )
    form_accession_number = models.TextField(
        db_column="form_accession_number"
    )
    form_report_period = models.DateField(db_column="form_report_period")
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
    form_url = models.URLField(db_column="form_url")
    exchange_code = models.TextField(db_column="exchange_code", null=True)
    issuer_name = models.TextField(db_column="issuer_name")
    cusip = models.CharField(db_column="cusip", max_length=9)
    title_class = models.TextField(db_column="title_class", null=True)
    market_sector = models.CharField(db_column="market_sector", max_length=255)
    security_type = models.CharField(db_column="security_type", max_length=255)
    ticker = models.CharField(db_column="ticker", max_length=255)
    value_x1000 = models.IntegerField(db_column="value_x1000")
    shares_prn_amt = models.IntegerField(db_column="shares_prn_amt")
    sh_prn = models.CharField(db_column="sh_prn", max_length=255)
    put_call = models.TextField(db_column="put_call", null=True)
    investment_discretion = models.CharField(
        db_column="investment_discretion", max_length=255
    )
    manager = models.TextField(db_column="manager", blank=True, default="")
    voting_auth_sole = models.IntegerField(
        db_column="voting_auth_sole",
        null=True
    )
    voting_auth_shared = models.IntegerField(
        db_column="voting_auth_shared",
        null=True
    )
    voting_auth_none = models.IntegerField(
        db_column="voting_auth_none", 
        null=True
    )
    num_shares = models.IntegerField(db_column="num_shares")
    principal_amount = models.IntegerField(db_column="principal_amount")
    created_at_utc = models.DateTimeField(
        db_column="created_at_utc", 
        auto_now_add=True,
        null=True
    )
