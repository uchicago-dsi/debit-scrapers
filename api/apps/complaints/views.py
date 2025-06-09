"""API views for development bank project complaints
collected by Accountability Counsel. 
"""

import json
from django.core.cache import cache
from django.db.models import Count, Sum
from django.db.models.functions import Coalesce, ExtractYear
from django.db.models import Count, Q, Sum
from django.db.models.expressions import F
from django.http import JsonResponse
from django.http.response import HttpResponseServerError
from ..banks.models import Bank
from ..complaints.models import Issue
from ..countries.models import Country
from ..common.functions import bulk_insert_records, bulk_upsert_records
from ..projects.models import Project, ProjectCountry, ProjectSector
from ..sectors.models import Sector
from rest_framework.views import APIView
from rest_framework.request import Request
from .models import Complaint, ComplaintIssue
from .serializers import (
    ComplaintSerializer, 
    ComplaintIssueSerializer, 
    IssueSerializer
)
from typing import Dict, List


DEFAULT_NUM_COMPLAINTS = 100
MAX_NUM_COMPLAINTS = 500
COMPLAINT_MAP_CACHE_KEY = "complaint-map"
COMPLAINT_REPORT_CACHE_KEY = "complaint-report"
NUM_COMPLAINT_MAP_BATCHES = 1


class ComplaintIssueListApiView(APIView):
    """Defines permitted operations for collections of complaint-issue pairs.
    """

    def post(self, request: Request) -> JsonResponse:
        """Bulk inserts one or more complaint-issues into the database,
        ignoring complaint-issues that already exist.

        Args:
            request (`Request`): The Django REST Framework request object.

        Returns:
            (`JsonResponse`): The ids of the newly-created complaint-issues.
        """
        try:
            # Extract properties and records from request payload
            complaint_issues = request.data.get("records", None)

            # Return error if no complaint-issue pairs provided
            if not complaint_issues and not isinstance(complaint_issues, list):
                return JsonResponse(
                    data="Expected to receive a list of complaint-issues.",
                    status=400,
                    safe=False)

             # Perform bulk insert of records
            dest_table_name = ComplaintIssueSerializer.Meta.model._meta.db_table
            field_names = ComplaintIssueSerializer.Meta.fields
            inserted_ids = bulk_insert_records(
                records=complaint_issues,
                dest_table_name=dest_table_name,
                dest_table_fields=field_names)
            
            # Clear cache if insert successful
            cache.clear()

            # Compose return payload and status
            status = 201 if inserted_ids else 200
            return JsonResponse(inserted_ids, status=status, safe=False)

        except Exception as e:
            return HttpResponseServerError(f"Failed to insert complaint-issues "
                f"in database. {e}")

class ComplaintListApiView(APIView):
    """Defines permitted operations for collections of complaints.
    """

    def get(self, request) -> JsonResponse:
        """Retrieves a subset of available complaints.

        Args:
            request (`Request`): The Django REST Framework request object.
                Contains "bank", "country", "sector", "offset", 
                and "limit" fields.

        Returns:
            (`JsonResponse`): The list of serialized complaint records.
        """
        # Extract query parameters from request object
        bank = request.GET.get("bank", None)
        country = request.GET.get("country", None)
        sector = request.GET.get("sector", None)
        issue = request.GET.get("issue", None)
        limit = request.GET.get("limit", None)
        offset = request.GET.get("offset", None)

        # Validate parameters
        if not limit or limit > MAX_NUM_COMPLAINTS:
            limit = DEFAULT_NUM_COMPLAINTS

        # Filter records by project bank and country
        projects = Project.objects
        complaints = Complaint.objects

        if bank:
            projects = projects.filter(bank_id=bank)

        if country:
            country_id = int(country)
            pcountries = ProjectCountry.objects.filter(country_id=country_id)
            project_ids = pcountries.values_list("project_id", flat=True)
            projects = projects.filter(pk__in=project_ids)

        if sector:
            sector_id = int(sector)
            psectors = ProjectSector.objects.filter(sector_id=sector_id)
            project_ids = psectors.values_list("project_id", flat=True)
            projects = projects.filter(pk__in=project_ids)

        if not bank and not country:
            projects = projects.all()

        project_ids = projects.values_list("id", flat=True)
        complaints = complaints.filter(project_id__in=project_ids)

        # Filter by complaint issue type
        if issue:
            issue_id = int(issue)
            cissues = ComplaintIssue.objects.filter(issue_id=issue_id)
            complaint_ids = cissues.values_list("complaint_id", flat=True)
            complaints = complaints.filter(pk__in=complaint_ids)

        # Order by id
        complaints = complaints.order_by("id")

        # Apply limit and, if provided, offset
        limit = int(limit)
        if offset and limit:
            offset = int(offset)
            complaint_subset = complaints[offset : offset + limit]
        else:
            complaint_subset = complaints[:limit]
        
        # Execute query 
        try:
            serializer = ComplaintSerializer(complaint_subset, many=True)
            return JsonResponse(serializer.data, status=200, safe=False)
        except Exception as e:
            return HttpResponseServerError("Failed to retrieve projects "
                f"from database. {e}")

    def post(self, request: Request) -> JsonResponse:
        """Inserts one or more complaints into the database
        or updates them if they already exist.

        Args:
            request (`Request`): The Django REST Framework request object.

        Returns:
            (`JsonResponse`): The complaints.
        """
        try:
            # Extract properties and records from request payload
            complaints = request.data.get("records", None)

            # Return error if no complaints provided
            if not complaints and not isinstance(complaints, list):
                return JsonResponse(
                    data="Expected to receive one or more complaints.",
                    status=400,
                    safe=False)

            # Perform bulk upsert of records
            dest_table_name = ComplaintSerializer.Meta.model._meta.db_table
            field_names = ComplaintSerializer.Meta.fields
            update_fields = [
                "complaint_status",
                "complaint_url",
                "filer_name",
                "filing_date",
                "is_registered",
                "registration_start",
                "registration_end",
                "registration_status",
                "no_registration_explanation",
                "issued_registration_report",
                "is_eligible",
                "eligibility_start",
                "eligibility_end",
                "eligibility_status",
                "no_eligibility_explanation",
                "issued_eligibility_report",
                "dispute_resolution_start",
                "dispute_resolution_end",
                "dispute_resolution_status",
                "no_dispute_resolution_explanation",
                "issued_dispute_report",
                "found_non_compliance",
                "is_monitored",
                "monitoring_start",
                "monitoring_end",
                "monitoring_status",
                "no_monitoring_explanation",
                "issued_monitoring_report",
                "issued_compliance_report",
                "compliance_review_start",
                "compliance_review_end",
                "compliance_review_status",
                "no_compliance_review_explanation",
                "has_agreement",
                "date_closed"
            ]
            constraint_name = "unique_complaint"
            complaints = bulk_upsert_records(
                records=complaints,
                dest_table_name=dest_table_name,
                dest_table_fields=field_names,
                unique_constraint_name=constraint_name,
                update_table_fields=update_fields)

            # Clear cache if upsert successful
            cache.clear()

            # Compose return payload and status
            status = 201 if complaints else 200
            return JsonResponse(complaints, status=status, safe=False)

        except Exception as e:
            return HttpResponseServerError(
                f"Failed to upsert complaints in database. {e}"
            )

class ComplaintMapApiView(APIView):
    """Defines permitted operations for the complaint geospatial map.
    """

    def get(self, request: Request) -> JsonResponse:
        """Retrieves a map of complaint, project, and
        project investment sums in USD by country.

        Args:
            request (`Request`): The Django REST Framework request object.

        Returns:
            (`JsonResponse`): The map as a GeoJSON FeatureCollection.
        """
        try:
            # Attempt to retrieve countries from cache
            complaints_map = cache.get(COMPLAINT_MAP_CACHE_KEY)
            if complaints_map:
                return JsonResponse(data=complaints_map, status=200, safe=False)

            # Aggregate project count, complaint count, and
            # USD investment by country
            country_queryset = Country.objects.annotate(
                total_num_funded_projects=Count("projects", distinct=True),
                total_funded_investments_in_usd=Coalesce(
                    Sum("projects__loan_amount_in_usd"), 0.0
                ),
                total_num_complaints=Count("projects__complaint", distinct=True)
            )

            # Select fields from query result
            countries = country_queryset.values(
                "id", 
                "name",
                "geojson",
                "total_num_funded_projects",
                "total_funded_investments_in_usd",
                "total_num_complaints"
            )

            # Generate country GeoJSON features
            features = []
            for c in countries:

                # Generate country GeoJSON feature
                features.append({
                    "type": "Feature",
                    "properties": {
                        "id": c["id"],
                        "name": c["name"],
                        "totalNumFundedProjects": c["total_num_funded_projects"],
                        "totalFundedInvestmentsInUsd": c["total_funded_investments_in_usd"],
                        "totalNumComplaints": c["total_num_complaints"]
                    },
                    "geometry": json.loads(c["geojson"].replace("\"", "\""))
                })

            # Manually construct GeoJSON feature collection
            complaint_map = {
                "type": "FeatureCollection",
                "crs": {
                    "type": "name",
                    "properties": { 
                        "name": "urn:ogc:def:crs:OGC:1.3:CRS84"
                    },
                },
                "features": features
            }

            # Update cache
            cache.set(COMPLAINT_MAP_CACHE_KEY, complaint_map)

            return JsonResponse(data=complaint_map, status=200, safe=False)

        except Exception as e:
            return JsonResponse(
                data=(
                    "Failed to aggregate complaints and projects "
                    f"projects by country. {e}"
                ),
                status=500,
                safe=False)

class ComplaintReportApiView(APIView):
    """Defines operations for reports of projects and complaints by bank.
    """

    def _aggregate_by_bank(self) -> List[Dict]:
        """Aggregates project and complaint data by bank.

        Args:
            `None`

        Returns:
            (`list` of `dict`): The data records.
        """
        # Aggregate project statistics
        total_num_funded_projects = Count("project__pk", distinct=True)
        total_funded_investments_in_usd = Coalesce(Sum("project__loan_amount_in_usd"), 0.0)

        # Aggregate project complaint statistics
        total_num_complaints = Count("project__complaint__pk", distinct=True)
        num_complaints_closed_with_outcome = (Count(
            "project__complaint__pk",
            distinct=True,
            filter=Q(project__complaint__complaint_status="Closed With Outputs")))
        num_complaints_closed_with_outside_outcome = (Count(
            "project__complaint__pk",
            distinct=True,
            filter=Q(project__complaint__complaint_status="Closed With Outputs Outside Process")))
        num_complaints_closed_without_outcome = (Count(
            "project__complaint__pk",
            distinct=True,
            filter=Q(project__complaint__complaint_status="Closed Without Outputs")))
        num_complaints_in_progress = (Count(
            "project__complaint__pk",
            distinct=True,
            filter=Q(project__complaint__complaint_status="Active")))
        num_complaints_monitored = (Count(
            "project__complaint__pk",
            distinct=True,
            filter=Q(project__complaint__complaint_status="Monitoring")))
        
        # Annotate banks with aggregations
        banks = Bank.objects.annotate(
            total_num_funded_projects=total_num_funded_projects,
            total_funded_investments_in_usd=total_funded_investments_in_usd,
            total_num_complaints=total_num_complaints,
            num_complaints_closed_with_outcome=num_complaints_closed_with_outcome,
            num_complaints_closed_with_outside_outcome=num_complaints_closed_with_outside_outcome,
            num_complaints_closed_without_outcome=num_complaints_closed_without_outcome,
            num_complaints_in_progress=num_complaints_in_progress,
            num_complaints_monitored=num_complaints_monitored,
            iam_year=F("bankiam__iam_year")
        )

        # Select fields to return
        selected_bank_fields = banks.values(
            "id",
            "name",
            "iam_year",
            "total_num_funded_projects",
            "total_funded_investments_in_usd",
            "total_num_complaints",
            "num_complaints_closed_with_outcome",
            "num_complaints_closed_without_outcome",
            "num_complaints_closed_with_outside_outcome",
            "num_complaints_in_progress",
            "num_complaints_monitored"
        )
        
        # Group bank complaints by year
        complaints_by_year = (Bank.objects
            .annotate(complaint_year=ExtractYear("project__complaint__filing_date"))
            .values("id", "complaint_year")
            .annotate(total_num_complaints=Coalesce(Count("project__complaint__pk", distinct=True), 0))
            .values("id", "complaint_year", "total_num_complaints"))

        # Group bank project funding by year
        funding_by_year = (Bank.objects
            .annotate(project_year=F("project__year"))
            .values("id", "project_year")
            .annotate(total_funding_usd=Coalesce(Sum("project__loan_amount_in_usd", distinct=True), 0.0))
            .values("id", "project_year", "total_funding_usd"))

        # Add nested properties to final payload
        payload = []
        for b in selected_bank_fields:
            b["complaints_by_year"] = []
            b["funding_by_year_in_usd"] = []
            for c in complaints_by_year:
                if b["id"] == c["id"] and c["complaint_year"]:
                    b["complaints_by_year"].append({
                        "year": c["complaint_year"],
                        "total_num_complaints": c["total_num_complaints"]
                    })

            for f in funding_by_year:
                if b["id"] == f["id"] and f["project_year"]:
                    b["funding_by_year_in_usd"].append({
                        "year": f["project_year"],
                        "total_amount": f["total_funding_usd"]
                    })

            payload.append(b)

        return payload

    def _aggregate_by_issue(self) -> List[Dict]:
        """
        Aggregates project and complaint data by complaint issue type
        (e.g., "Biodiversity", "Community health and safety").

        Args:
            `None`

        Returns:
            (`list` of `dict`): The data records.
        """
        # Aggregate project complaint statistics
        total_num_complaints = Count("complaint__pk", distinct=True)
        num_complaints_closed_with_outcome = (Count(
            "complaint__pk",
            distinct=True,
            filter=Q(complaint__complaint_status="Closed With Outputs")))
        num_complaints_closed_with_outside_outcome = (Count(
            "complaint__pk",
            distinct=True,
            filter=Q(complaint__complaint_status="Closed With Outputs Outside Process")))
        num_complaints_closed_without_outcome = (Count(
            "complaint__pk",
            distinct=True,
            filter=Q(complaint__complaint_status="Closed Without Outputs")))
        num_complaints_in_progress = (Count(
            "complaint__pk",
            distinct=True,
            filter=Q(complaint__complaint_status="Active")))
        num_complaints_monitored = (Count(
            "complaint__pk",
            distinct=True,
            filter=Q(complaint__complaint_status="Monitoring")))

        # Annotate issues with aggregations
        issues = Issue.objects.annotate(
            total_num_complaints=total_num_complaints,
            num_complaints_closed_with_outcome=num_complaints_closed_with_outcome,
            num_complaints_closed_with_outside_outcome=num_complaints_closed_with_outside_outcome,
            num_complaints_closed_without_outcome=num_complaints_closed_without_outcome,
            num_complaints_in_progress=num_complaints_in_progress,
            num_complaints_monitored=num_complaints_monitored
        )

        # Select fields to return
        selected_issue_fields = issues.order_by("-total_num_complaints").values(
            "id",
            "name",
            "total_num_complaints",
            "num_complaints_closed_with_outcome",
            "num_complaints_closed_without_outcome",
            "num_complaints_closed_with_outside_outcome",
            "num_complaints_in_progress",
            "num_complaints_monitored"
        )

        # Convert to list of dictionaries
        payload = [i for i in selected_issue_fields]

        return payload

    def _aggregate_by_sector(self) -> List[Dict]:
        """
        Aggregates project and complaint data by sector.

        Args:
            `None`

        Returns:
            (`list` of `dict`): The data records.
        """
        # Aggregate project statistics
        total_num_funded_projects = Count("projects__pk", distinct=True)
        total_funded_investments_in_usd = Coalesce(Sum("projects__loan_amount_in_usd"), 0.0)

        # Aggregate project complaint statistics
        total_num_complaints = Count("projects__complaint__pk", distinct=True)
        num_complaints_closed_with_outcome = (Count(
            "projects__complaint__pk",
            distinct=True,
            filter=Q(projects__complaint__complaint_status="Closed With Outputs")))
        num_complaints_closed_with_outside_outcome = (Count(
            "projects__complaint__pk",
            distinct=True,
            filter=Q(projects__complaint__complaint_status="Closed With Outputs Outside Process")))
        num_complaints_closed_without_outcome = (Count(
            "projects__complaint__pk",
            distinct=True,
            filter=Q(projects__complaint__complaint_status="Closed Without Outputs")))
        num_complaints_in_progress = (Count(
            "projects__complaint__pk",
            distinct=True,
            filter=Q(projects__complaint__complaint_status="Active")))
        num_complaints_monitored = (Count(
            "projects__complaint__pk",
            distinct=True,
            filter=Q(projects__complaint__complaint_status="Monitoring")))
        
        # Annotate sectors with aggregations
        sectors = Sector.objects.annotate(
            total_num_funded_projects=total_num_funded_projects,
            total_funded_investments_in_usd=total_funded_investments_in_usd,
            total_num_complaints=total_num_complaints,
            num_complaints_closed_with_outcome=num_complaints_closed_with_outcome,
            num_complaints_closed_with_outside_outcome=num_complaints_closed_with_outside_outcome,
            num_complaints_closed_without_outcome=num_complaints_closed_without_outcome,
            num_complaints_in_progress=num_complaints_in_progress,
            num_complaints_monitored=num_complaints_monitored
        )

        # Select fields to return
        selected_sector_fields = sectors.values(
            "id",
            "name",
            "total_num_funded_projects",
            "total_funded_investments_in_usd",
            "total_num_complaints",
            "num_complaints_closed_with_outcome",
            "num_complaints_closed_without_outcome",
            "num_complaints_closed_with_outside_outcome",
            "num_complaints_in_progress",
            "num_complaints_monitored"
        )

        # Group sector by issue type
        issue_count_by_sector = (Sector.objects
            .annotate(
                sector_id=F("id"),
                issue_id=F("projects__complaint__issues__pk"),
                issue_name=F("projects__complaint__issues__name")
            )
            .values("sector_id", "issue_id", "issue_name")
            .annotate(issue_count=Coalesce(Count("issue_id"), 0))
            .values("sector_id", "issue_id", "issue_name", "issue_count"))

        # Create nested payload
        payload = []
        for s in selected_sector_fields:
            s["complaint_issue_counts"] = []
            for icount in issue_count_by_sector:
                if s["id"] == icount["sector_id"] and icount["issue_id"]:
                    s["complaint_issue_counts"].append({
                        "id": icount["issue_id"],
                        "name": icount["issue_name"],
                        "count": icount["issue_count"]
                    })
            payload.append(s)
                
        return payload
    
    def get(self, request: Request) -> JsonResponse:
        """Generates statistics by aggregating project complaints
        by bank, issue type, and sector.

        Args:
            request (`Request`): The Django REST Framework request object.

        Returns:
            (`list` of `dict`): The data records.
        """
        try:
            # Check cache for value and return if found
            complaint_report = cache.get(COMPLAINT_REPORT_CACHE_KEY)
            if complaint_report:
                return JsonResponse(data=complaint_report, status=200, safe=False)

            # Compute value if not already cached
            complaint_report = {}
            complaint_report["banks"] = self._aggregate_by_bank()
            complaint_report["issues"] = self._aggregate_by_issue()
            complaint_report["sectors"] = self._aggregate_by_sector()

            # Persist value to cache
            cache.set(COMPLAINT_REPORT_CACHE_KEY, complaint_report)

            return JsonResponse(data=complaint_report, status=200, safe=False)

        except Exception as e:
            return JsonResponse(
                data=f"Failed to generate complaints report. {e}",
                status=500,
                safe=False)

class IssueListApiView(APIView):
    """Defines permitted operations for collections of issues.
    """

    def post(self, request: Request) -> JsonResponse:
        """Inserts one or more issues into the database.

        Args:
            request (`Request`): The Django REST Framework request object.

        Returns:
            (`JsonResponse`): The newly-created issues.
        """
        try:
            # Extract records from request payload
            issues = request.data.get("records", None)

            # Return error if no issues provided
            if not issues:
                return JsonResponse(
                    data="Expected to receive one or more issues.",
                    status=400,
                    safe=False)

            # Perform bulk insert of records
            dest_table_name = IssueSerializer.Meta.model._meta.db_table
            field_names = ["name"]
            inserted_ids = bulk_upsert_records(
                records=issues,
                dest_table_name=dest_table_name,
                dest_table_fields=field_names,
                unique_constraint_name="unique_issue_name",
                update_table_fields=["name"])

            # Clear cache if upsert successful
            cache.clear()
            
            # Compose return payload and status
            status = 201 if inserted_ids else 200
            return JsonResponse(inserted_ids, status=status, safe=False)

        except Exception as e:
            return HttpResponseServerError(f"Failed to insert issues in database. {e}")
