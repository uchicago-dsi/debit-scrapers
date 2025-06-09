"""Serializers for projects and their associations.
"""

from .models import Project, ProjectCountry, ProjectSector
from rest_framework import serializers


class ProjectSerializer(serializers.ModelSerializer):
    """A serializer for development bank projects.
    """
    
    class Meta:
        model = Project
        fields = [
            "id",
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
            "sector_list_raw",
            "sector_list_stnd",
            "companies",
            "country_list_raw",
            "country_list_stnd",
            "url"
        ]

class ProjectCountrySerializer(serializers.ModelSerializer):
    """A serializer for project-country associations.
    """
    
    class Meta:
        model = ProjectCountry
        fields = [
            "project",
            "country",
        ]

class ProjectSectorSerializer(serializers.ModelSerializer):
    """A serializer for project-sector associations
    """
    
    class Meta:
        model = ProjectSector
        fields = [
            "project",
            "sector",
        ]