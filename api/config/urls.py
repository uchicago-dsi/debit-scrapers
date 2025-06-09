"""api URL Configuration

The `urlpatterns` list routes URLs to views. For more information please see:
    https://docs.djangoproject.com/en/3.2/topics/http/urls/
Examples:
Function views
    1. Add an import:  from my_app import views
    2. Add a URL to urlpatterns:  path('', views.home, name='home')
Class-based views
    1. Add an import:  from other_app.views import Home
    2. Add a URL to urlpatterns:  path('', Home.as_view(), name='home')
Including another URLconf
    1. Import the include() function: from django.urls import include, path
    2. Add a URL to urlpatterns:  path('blog/', include('blog.urls'))
"""
from django.contrib import admin
from django.urls import path
from apps.banks.views import (
    BankListApiView
)
# from apps.complaints.views import (
#     ComplaintIssueListApiView,
#     ComplaintListApiView,
#     ComplaintMapApiView,
#     ComplaintReportApiView,
#     IssueListApiView
# )
from apps.countries.views import (
    CountryListApiView
)
from apps.form13f.views import (
    CompanyListApiView,
    InvestmentListApiView,
    InvestmentSearchApiView,
    SubmissionListApiView
)
from apps.pipeline.views import (
    JobApiView,
    # JobListApiView,
    StagedEquityInvestmentListApiView,
    StagedProjectByUrlViewSet,
    StagedProjectListApiView,
    TaskDetailApiView,
    TaskListApiView
)
from apps.projects.views import (
    ProjectCountryListApiView,
    ProjectDetailApiView,
    ProjectListApiView,
    ProjectSearchApiView,
    ProjectSectorBulkCreateApiView
)
from apps.sectors.views import (
    SectorListApiView
)
from drf_spectacular.views import (
    SpectacularAPIView,
    SpectacularRedocView
)


from rest_framework import routers

router = routers.SimpleRouter(trailing_slash=False)
router.register(r'api/pipeline/jobs', JobApiView, basename="jobs")


urlpatterns = [
    # ADMIN
    path('admin/', admin.site.urls),

    # BANKS
    path("api/banks", BankListApiView.as_view(), name='bank-list'),

    # # COMPLAINTS
    # path("api/complaints", ComplaintListApiView.as_view(), name="complaint-list"),
    # path("api/complaints/issues", IssueListApiView.as_view(), name='issue-list'),
    # path("api/complaints/complaint-issues", ComplaintIssueListApiView.as_view(), name='complaint-issue-list'),
    # path("api/complaints/complaint-map", ComplaintMapApiView.as_view(), name='complaint-map'),
    # path("api/complaints/complaint-report", ComplaintReportApiView.as_view(), name='complaint-report'),

    # COUNTRIES
    path("api/countries", CountryListApiView.as_view(), name='country-list'),

    # FORM 13F
    path("api/form13f/companies", CompanyListApiView.as_view(), name='form-13f-companies-list'),
    path("api/form13f/forms", SubmissionListApiView.as_view(), name="form-13f-submissions-list"),
    path("api/form13f/investments", InvestmentListApiView.as_view(), name='form-13f-investments-list'),
    path("api/form13f/investments/search", InvestmentSearchApiView.as_view(), name='form-13f-investments-search'),

    # PIPELINE
    # path("api/pipeline/jobs/<int:pk>", JobApiView.as_view(), name="job-detail"),
    # path("api/pipeline/jobs", JobListApiView.as_view(), name="job-list"),
    path("api/pipeline/staged-investments", StagedEquityInvestmentListApiView.as_view(), name="staged-investment-list"),
    path("api/pipeline/staged-projects", StagedProjectListApiView.as_view(), name="staged-project-list"),
    path("api/pipeline/staged-projects/delete-by-url", StagedProjectByUrlViewSet.as_view({'delete': 'delete'}), name="staged-project-delete-by-url"),
    path("api/pipeline/staged-projects/search-by-url", StagedProjectByUrlViewSet.as_view({'post': 'search'}), name="staged-project-search-by-url"),
    path("api/pipeline/tasks", TaskListApiView.as_view(), name="task-list"),
    path("api/pipeline/tasks/<task_id>", TaskDetailApiView.as_view(), name="task-detail"),

    # PROJECTS
    path("api/projects", ProjectListApiView.as_view(), name="project-list"),
    path("api/projects/countries", ProjectCountryListApiView.as_view(), name="project-country-list"),
    path("api/projects/search", ProjectSearchApiView.as_view(), name="project-search"),
    path("api/projects/<int:pk>", ProjectDetailApiView.as_view(), name="project-detail"),

    # SECTORS
    path("api/sectors", SectorListApiView.as_view(), name="sector-list"),    
    path("api/sectors/project-sectors", ProjectSectorBulkCreateApiView.as_view(), name="project-sector-list"),

    # DOCUMENTATION
    path("api/schema", SpectacularAPIView.as_view(), name="schema"),
    path("api/schema/redoc", SpectacularRedocView.as_view(url_name="schema"), name="redoc"),
]

urlpatterns += router.urls