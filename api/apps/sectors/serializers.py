"""Serializers for sectors and their associations.
"""

from .models import Sector
from rest_framework import serializers


class SectorSerializer(serializers.ModelSerializer):
    """A serializer for sectors.
    """
    
    class Meta:
        model = Sector
        fields = [
           "id",
           "name"
        ]

