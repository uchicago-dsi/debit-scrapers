"""Serializers for pipeline entities.
"""

from apps.common.serializers import BulkUpsertSerializer, CreateOrIgnoreSerializer
from .models import Job, StagedEquityInvestment, StagedProject, Task
from rest_framework import serializers


class JobSerializer(CreateOrIgnoreSerializer):
    """A serializer for pipeline processing jobs.
    """
    
    class Meta:
        model = Job
        fields = [
            "id",
            "invocation_id",
            "job_type",
            "data_load_stage",
            "data_load_start_utc",
            "data_load_end_utc",
            "data_clean_stage",
            "data_clean_start_utc",
            "data_clean_end_utc",
            "raw_url",
            "clean_url",
            "audit_url",
            "created_at_utc"         
        ]
        unique_fields = ["invocation_id"]

class StagedEquityInvestmentSerializer(serializers.ModelSerializer):
    """A serializer for staged Form13F investments.
    """
    
    class Meta:
        model = StagedEquityInvestment
        fields = [
            "id",
            "task",
            "company_cik",
            "company_name",
            "form_name",
            "form_accession_number",
            "form_report_period",
            "form_filing_date",
            "form_acceptance_date",
            "form_effective_date",
            "form_url",
            "stock_issuer_name",
            "stock_title_class",
            "stock_cusip",
            "stock_value_x1000",
            "stock_shares_prn_amt",
            "stock_sh_prn",
            "stock_put_call",
            "stock_investment_discretion",
            "stock_manager",
            "stock_voting_auth_sole",
            "stock_voting_auth_shared",
            "stock_voting_auth_none"
        ]
        list_serializer_class = BulkUpsertSerializer

class StagedProjectSerializer(serializers.ModelSerializer):
    """A serializer for staged development bank projects.
    """
    
    class Meta:
        model = StagedProject
        fields = [
            "id",
            "task",
            "bank",
            "number",
            "name",
            "status",
            "year",
            "month",
            "day",
            "loan_amount",
            "loan_amount_currency",
            "loan_amount_in_usd",
            "sectors",
            "countries",
            "companies",
            "url"
        ]
        list_serializer_class = BulkUpsertSerializer

class TaskSerializer(serializers.ModelSerializer):
    """A serializer for pipeline processing tasks.
    """
    
    class Meta:
        model = Task
        fields = [
            "id",
            "job",
            "status",
            "source",
            "url",
            "workflow_type",
            "created_at_utc",
            "processing_start_utc",
            "processing_end_utc",
            "scraping_start_utc",
            "scraping_end_utc",
            "last_failed_at_utc",
            "last_error_message",
            "retry_count"
        ]
        extra_kwargs = {"url": {"allow_null": True}} 
        list_serializer_class = BulkUpsertSerializer
