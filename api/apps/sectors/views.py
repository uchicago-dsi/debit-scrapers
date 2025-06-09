"""API views for sectors.
"""

from .models import Sector
from rest_framework import generics
from .serializers import SectorSerializer


class SectorListApiView(generics.ListAPIView):
    """Returns a paginated view of sector records.
    """
    queryset = Sector.objects.all()
    serializer_class = SectorSerializer
    error_message = "Failed to retrieve sectors from database."
