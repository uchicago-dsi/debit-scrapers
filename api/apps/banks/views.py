"""API views for development banks.
"""

from .models import Bank
from .serializers import BankSerializer

from rest_framework import generics


class BankListApiView(generics.ListAPIView):
    """Returns a paginated view of all banks in the database.
    """
    queryset = Bank.objects.all()
    serializer_class = BankSerializer
    error_message = "Failed to retrieve banks from database."
