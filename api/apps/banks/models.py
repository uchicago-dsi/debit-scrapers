"""Data models for development banks.
"""

from django.db import models


class Bank(models.Model):
    """Bank metadata.
    """
        
    class Meta:
        db_table = "bank"
        
    id = models.BigIntegerField(
        db_column="id",
        primary_key=True
    )
    name = models.TextField(
        db_column="name"
    )
    abbreviation = models.TextField(
        db_column="abbreviation"
    )
    created_at_utc = models.DateTimeField(
        db_column="created_at_utc", 
        auto_now_add=True, 
        null=True
    )

class IAM(models.Model):
    """Metadata for independent accountability mechanisms (IAMs).
    """
    
    class Meta:
        db_table = "iam"

    id = models.BigIntegerField(
        db_column="id",
        primary_key=True
    )
    name = models.TextField(
        db_column="name"
    )
    created_at_utc = models.DateTimeField(
        db_column="created_at_utc",
        auto_now_add=True,
        null=True
    )

class BankIAM(models.Model):
    """Bank-IAM associations.
    """
    
    class Meta:
        db_table = "bank_iam"

    bank = models.ForeignKey(
        to="banks.Bank",
        db_column="bank",
        on_delete=models.CASCADE
    )
    iam = models.ForeignKey(
        to="banks.IAM",
        db_column="iam",
        on_delete=models.CASCADE
    )
    iam_year = models.IntegerField(
        db_column="iam_year",
        null=True
    )
    iam_month = models.IntegerField(
        db_column="iam_month",
        null=True
    )
    iam_day = models.IntegerField(
        db_column="iam_day",
        null=True
    )
    iam_date_notes = models.TextField(
        db_column="iam_date_notes",
        null=True
    )
    created_at_utc = models.DateTimeField(
        db_column="created_at_utc", 
        auto_now_add=True, 
        null=True
    )
