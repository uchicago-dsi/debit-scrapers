"""Serializers for Form 13F entities.
"""

from .models import Form13FCompany, Form13FInvestment, Form13FSubmission
from rest_framework import serializers


class Form13FCompanySerializer(serializers.ModelSerializer):
    """A serializer for companies.
    """  

    class Meta:
        model = Form13FCompany
        fields = [
           "id",
           "cik",
           "name"
        ]

class Form13FSubmissionSerializer(serializers.ModelSerializer):
    """A serializer for Form 13F submissions.
    """

    class Meta:
        model = Form13FSubmission
        fields = [
           "id",
           "company",
           "name",
           "accession_number",
           "report_period",
           "filing_date",
           "acceptance_date",
           "effective_date",
           "url"
        ]


class Form13FInvestmentSerializer(serializers.ModelSerializer):
    """A serializer for Form 13F investments.
    """

    class Meta:
        model = Form13FInvestment
        fields = [
           "id",
           "form",
           "exchange_code",
           "issuer_name",
           "cusip",
           "title_class",
           "market_sector",
           "security_type",
           "ticker",
           "value_x1000",
           "shares_prn_amt",
           "sh_prn",
           "put_call",
           "investment_discretion",
           "manager",
           "voting_auth_sole",
           "voting_auth_shared",
           "voting_auth_none"
        ]
