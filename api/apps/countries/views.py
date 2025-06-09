
"""API views for countries.
"""
from .models import Country
from rest_framework import generics
from .serializers import CountryListSerializer


class CountryListApiView(generics.ListAPIView):
    """Returns a paginated view of country records
    without project relationships or GeoJSON boundaries.
    """
    queryset = Country.objects.defer("projects", "geojson")
    serializer_class = CountryListSerializer
    error_message = "Failed to retrieve countries from database."
