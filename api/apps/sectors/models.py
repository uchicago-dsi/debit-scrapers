"""Data models for sectors.
"""

from django.db import models


class Sector(models.Model):
    """Metadata for a sector.
    """
    
    class Meta:
        db_table = "sector"
        constraints = [
            models.UniqueConstraint(
                fields=["name"], 
                name="unique_sector"
            )
        ]
        
    name = models.TextField(
		db_column="name"
    )
    projects = models.ManyToManyField(
        to="projects.Project",
        through="projects.ProjectSector"
    )
    created_at_utc = models.DateTimeField(
		db_column="created_at_utc",
		auto_now_add=True,
		null=True
	)
