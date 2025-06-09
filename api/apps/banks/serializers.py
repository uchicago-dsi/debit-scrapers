"""Serializers for development banks and their associations.
"""

from .models import Bank
from rest_framework import serializers


class BankSerializer(serializers.ModelSerializer):
    """A serializer for development banks.
    """
    
    class Meta:
        model = Bank
        fields = [
           "id",
           "name",
           "abbreviation"
        ]