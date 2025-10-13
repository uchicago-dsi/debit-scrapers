"""Routes URLs to Django project views."""

# Third-party imports
from django.contrib import admin
from django.urls import path

# Application imports
from extract.views import ExtractionWorkflowView

urlpatterns = [
    path("admin/", admin.site.urls),
    path(
        "api/v1/gcp/tasks",
        ExtractionWorkflowView.as_view(),
        name="google-cloud-tasks",
    ),
]
