"""Serializers for countries and their associations.
"""

from ..common.serializers import DynamicFieldsSerializer
from .models import Country


class CountryListSerializer(DynamicFieldsSerializer):
    """A serializer for countries.
    """
    
    class Meta:
        model = Country
        fields = [
           "id",
           "name",
           "iso_code"
        ]
