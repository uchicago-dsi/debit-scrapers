"""Serializers for complaints and their associations.
"""

from ..common.serializers import BulkUpsertSerializer
from .models import Complaint, ComplaintIssue, Issue
from rest_framework import serializers


class ComplaintSerializer(serializers.ModelSerializer):
    """A serializer for complaints from Accountability Counsel.
    """
    
    class Meta:
        model = Complaint
        fields = [
            "project",
            "complaint_name",
            "complaint_status",
            "complaint_url",
            "filer_name",
            "filing_date",
            "is_registered",
            "registration_start",
            "registration_end",
            "registration_status",
            "no_registration_explanation",
            "issued_registration_report",
            "is_eligible",
            "eligibility_start",
            "eligibility_end",
            "eligibility_status",
            "no_eligibility_explanation",
            "issued_eligibility_report",
            "dispute_resolution_start",
            "dispute_resolution_end",
            "dispute_resolution_status",
            "no_dispute_resolution_explanation",
            "issued_dispute_report",
            "found_non_compliance",
            "is_monitored",
            "monitoring_start",
            "monitoring_end",
            "monitoring_status",
            "no_monitoring_explanation",
            "issued_monitoring_report",
            "issued_compliance_report",
            "compliance_review_start",
            "compliance_review_end",
            "compliance_review_status",
            "no_compliance_review_explanation",
            "has_agreement",
            "date_closed",
        ]
        list_serializer_class = BulkUpsertSerializer

class ComplaintIssueSerializer(serializers.ModelSerializer):
    """A serializer for complaint-issue association pairs.
    """
    
    class Meta:
        model = ComplaintIssue
        fields = [
           "complaint",
           "issue"
        ]
        list_serializer_class = BulkUpsertSerializer

class IssueSerializer(serializers.ModelSerializer):
    """A serializer for complaint issue types.
    """
    
    class Meta:
        model = Issue
        fields = [
           "id",
           "name"
        ]
        list_serializer_class = BulkUpsertSerializer
