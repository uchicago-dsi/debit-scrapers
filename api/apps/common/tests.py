"""Mixins used alongside test cases.
"""

from django.urls import reverse
from rest_framework import status


class ListTestMixin:
    """Provides generic tests for `ListAPIView` subclasses.
    """

    def test_list_objects(self):
        """Asserts that all objects in the queryset
        can be successfully fetched from the database.
        """
        # Count number of objects loaded into test database
        num_objects = self.model.objects.count()

        # Call API to fetch all records, with desired fields
        url = reverse(self.view_name)
        response = self.client.get(url, format="json")
        data = response.json()

        # Assert that records were successfully returned
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(data), num_objects)
