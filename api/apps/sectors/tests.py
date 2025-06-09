"""Tests for the sectors application.
"""

from rest_framework.test import APITestCase
from apps.common.tests import ListTestMixin
from apps.sectors.models import Sector

class SectorTests(APITestCase, ListTestMixin):
    """Test cases for sectors.
    """
    fixtures = ["sector.json"]
    model = Sector
    view_name = "sector-list"
