"""Tests for the countries application.
"""

from rest_framework.test import APITestCase
from apps.common.tests import ListTestMixin
from apps.countries.models import Country

class CountryTests(APITestCase, ListTestMixin):
    """Test cases for countries.
    """
    fixtures = ["country.json"]
    model = Country
    view_name = "country-list"
