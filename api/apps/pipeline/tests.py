"""Tests for the pipeline application.
"""

from django.urls import reverse
from rest_framework import status
from rest_framework.test import APITestCase
from apps.pipeline.factories import JobFactory

class PipelineTests(APITestCase):
    """Test cases for banks.
    """
    
    def test_get_job(self):
        """Asserts that a job can be fetched from the database by id.
        """
        # Create job in database
        job = JobFactory.create()

        # Fetch same project using API
        url = reverse("job-detail", args=[job.id])
        response = self.client.get(url, format="json")
        print(response.content)
        data = response.json()

        # Assert that the project was successfully returned
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(data.get("id"), job.id)
