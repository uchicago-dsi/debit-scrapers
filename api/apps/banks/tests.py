"""Tests for the banks application.
"""

from rest_framework.test import APITestCase
from apps.common.tests import ListTestMixin
from apps.banks.models import Bank

class BankTests(APITestCase, ListTestMixin):
    """Test cases for sectors.
    """
    fixtures = ["bank.json"]
    model = Bank
    view_name = "bank-list"
