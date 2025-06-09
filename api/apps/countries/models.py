"""Data models for countries.
"""

from django.db import models

class Country(models.Model):
    """Metadata for countries.
    """
    
    class Meta:
        db_table = "country"
        
    name = models.TextField(
        db_column="name"
    )
    iso_code = models.TextField(
        db_column="iso_code"
    )
    projects = models.ManyToManyField(
        to="projects.Project",
        through="projects.ProjectCountry"
    )
    geojson = models.TextField(
        db_column="geojson"
    )
    created_at_utc = models.DateTimeField(
        db_column="created_at_utc",
        auto_now_add=True,
        null=True
    )
