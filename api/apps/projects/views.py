"""API views for projects.
"""

import json
from ..common.functions import bulk_insert_records, bulk_upsert_records
from django.db.models import Q
from django.db.models.functions import Upper
from django.http import JsonResponse, StreamingHttpResponse
from .models import Project, ProjectSector
from rest_framework.views import APIView
from rest_framework.request import Request
from .serializers import (
    ProjectSerializer,
    ProjectCountrySerializer,
    ProjectSectorSerializer
)

from apps.common.views import BulkCreateApiView
from rest_framework import generics


class ProjectDetailApiView(generics.RetrieveAPIView):
    """REST API operations for single projects.
    """
    queryset = Project.objects.all()
    serializer_class = ProjectSerializer
    error_message = "Error fetching project."

class ProjectSearchApiView(APIView):
    """REST API operations for project queries.
    """

    def post(self, request: Request) -> JsonResponse:
        """Retrieves a subset of available projects
        using a search query.

        Args:
            request (`Request`): The Django REST Framework request object.

        Returns:
            (`JsonResponse`): The list of serialized project records.
        """
        # Extract settings from request object
        name = request.data.get("name", None)
        bank = request.data.get("bank", None)
        country = request.data.get("countries", None)
        sector = request.data.get("sectors", None)
        company = request.data.get("companies", None)
        status = request.data.get("status", None)
        search = request.data.get("search", None)
        limit = request.data.get("limit", None)
        offset = request.data.get("offset", None)
        sort_col = request.data.get("sortCol", None)
        sort_desc = request.data.get("sortDesc", 0)
        is_download = request.data.get("download", 0)

        # Join projects with banks to get bank name
        projects = Project.objects.annotate(
            bank_abbrev=Upper("bank__abbreviation")
        )

        # Filter records by name, bank, country, sector, and/or status
        if name:
            projects = projects.filter(name__icontains=name)

        if bank:
            projects = projects.filter(bank_name__icontains=bank)

        if country:
            country_search = Q(country_list_raw__icontains=country) | \
                Q(country_list_stnd__icontains=country)
            projects = projects.filter(country_search)

        if sector:
            sector_search = Q(sector_list_raw__icontains=sector) | \
                Q(sector_list_stnd__icontains=sector)
            projects = projects.filter(sector_search)

        if status:
            projects = projects.filter(status__icontains=status)

        if company:
            projects = projects.filter(companies__icontains=company)

        if search:
            search_query = Q()
            search_query |= Q(name__icontains=search)
            search_query |= Q(bank_abbrev__icontains=search)
            search_query |= Q(bank__name__icontains=search)
            search_query |= Q(status__icontains=search)
            search_query |= Q(loan_amount_in_usd__icontains=search)
            search_query |= Q(country_list_raw__icontains=search)
            search_query |= Q(country_list_stnd__icontains=search)
            search_query |= Q(sector_list_raw__icontains=search)
            search_query |= Q(sector_list_stnd__icontains=search)
            search_query |= Q(companies__icontains=search)
            projects = projects.filter(search_query)

        if not (bank or country or name or status or sector or search):
            projects = projects.all()

        # Order by requested column, or use project name by default
        sort_col = sort_col if sort_col else "name"
        sort_prefix = "-" if sort_desc == 1 else ""
        if sort_col == "date":
            projects = projects.order_by(
                f"{sort_prefix}year",
                f"{sort_prefix}month",
                f"{sort_prefix}day"
            )
        elif sort_col == "bank":
            projects = projects.order_by(f"{sort_prefix}bank_name")
        elif sort_col == "name":
            projects = projects.order_by(f"{sort_prefix}name")
        elif sort_col == "status":
            projects = projects.order_by(f"{sort_prefix}status")
        elif sort_col == "loanAmountInUsd":
            projects = projects.order_by(f"{sort_prefix}loan_amount_in_usd")
        elif sort_col == "countries":
            projects = projects.order_by(f"{sort_prefix}country_list_stnd")
        elif sort_col == "sectors":
            projects = projects.order_by(f"{sort_prefix}sector_list_stnd")
        elif sort_col == "companies":
            projects = projects.order_by(f"{sort_prefix}companies")
        else:
            raise ValueError(f"Invalid sort column, \"{sort_col}\", received.")

        # Calculate total number of rows before subsetting
        total_num_rows = projects.count()

        # Validate limit parameter
        if limit:
            limit = int(limit)

        # Apply limit and, if provided, offset
        if offset and limit:
            offset = int(offset)
            project_subset = projects[offset : offset + limit]
        else:
            project_subset = projects[:limit]

        # Execute query 
        try:
            project_dicts = [
                p for p in
                project_subset.values(
                    "id",
                    "bank",
                    "bank_name",
                    "number",
                    "name",
                    "status",
                    "year",
                    "month",
                    "day",
                    "loan_amount",
                    "loan_amount_currency",
                    "loan_amount_in_usd",
                    "sector_list_raw",
                    "sector_list_stnd",
                    "companies",
                    "country_list_raw",
                    "country_list_stnd",
                    "url"
                )
            ]
        except Exception as e:
            return JsonResponse(
                data=f"Failed to retrieve projects from database. {e}",
                status=500,
                safe=False)

        # If not a download request, or no projects available, return JsonResponse
        if not is_download:
            payload = {
                "total_num_rows": total_num_rows,
                "projects": project_dicts
            }
            return JsonResponse(payload, status=200, safe=False)

        # Otherwise, initialize download
        try:
            def formatJson(j, idx):
                if idx == 0:
                    return "[" + json.dumps(j) + ","
                elif idx == len(project_dicts) - 1:
                    return json.dumps(j) + "]"
                else:
                    return json.dumps(j) + ","

            return StreamingHttpResponse(
                streaming_content=(
                    (formatJson(p, idx) for idx, p in enumerate(project_dicts)) 
                    if project_dicts else "[]"
                ),
                content_type="application/json"
            )
        except Exception as e:
            return JsonResponse(
                data=f"Failed to write projects to CSV file. {e}",
                status=500,
                safe=False
            )

class ProjectListApiView(APIView):
    """REST API operations for collections of projects.
    """

    def post(self, request: Request) -> JsonResponse:
        """Inserts or upserts one or more finalized
        project records into the database.

        Args:
            request (`Request`): The Django REST Framework request object.

        Returns:
            (`JsonResponse`): The list of serialized, newly-created
                staged project records.
        """
        try:
            # Extract records from request payload
            projects = request.data.get("records", None)

            # Return error if no projects provided
            if not projects and not isinstance(projects, list):
                return JsonResponse(
                    data="Expected to receive a list of projects.",
                    status=400,
                    safe=False)

            # Perform bulk upsert of records
            dest_table_name = ProjectSerializer.Meta.model._meta.db_table
            field_names = [
                "bank",
                "number",
                "name",
                "status",
                "year",
                "month",
                "day",
                "loan_amount",
                "loan_amount_currency",
                "loan_amount_in_usd",
                "sector_list_raw",
                "sector_list_stnd",
                "country_list_raw",
                "country_list_stnd",
                "companies",
                "url"
            ]
            update_fields = [
                "name",
                "status",
                "year",
                "month",
                "day",
                "loan_amount",
                "loan_amount_currency",
                "loan_amount_in_usd",
                "sector_list_raw",
                "sector_list_stnd",
                "country_list_raw",
                "country_list_stnd",
                "companies"
            ]
            constraint_name = "unique_bank_url"
            upserted_projects = bulk_upsert_records(
                records=projects,
                dest_table_name=dest_table_name,
                dest_table_fields=field_names,
                unique_constraint_name=constraint_name,
                update_table_fields=update_fields)

            # Compose return payload and status
            return JsonResponse(upserted_projects, status=200, safe=False)
        
        except Exception as e:
            return JsonResponse(
                data=f"Failed to insert finalized projects into database. {e}",
                status=500,
                safe=False
            )

class ProjectCountryListApiView(APIView):
    """REST API operations for collections of project-country pairs.
    """

    def post(self, request: Request) -> JsonResponse:
        """Inserts one or more project-country records into the database.

        Args:
            request (`Request`): The Django REST Framework request object.

        Returns:
            (`JsonResponse`): The list of serialized, newly-created
                project-country records.
        """
        try:
            # Extract records from request payload
            p_countries = request.data.get("records", None)

            # Return error if no project-country pairs provided
            if not p_countries and not isinstance(p_countries, list):
                return JsonResponse(
                    data="Expected to receive a list of project-countries.",
                    status=400,
                    safe=False)

            # Perform bulk insert of records
            dest_table_name = ProjectCountrySerializer.Meta.model._meta.db_table
            field_names = ProjectCountrySerializer.Meta.fields
            inserted_ids = bulk_insert_records(
                records=p_countries,
                dest_table_name=dest_table_name,
                dest_table_fields=field_names)

            # Compose return payload and status
            return JsonResponse(inserted_ids, status=200, safe=False)

        except Exception as e:
            return JsonResponse(
                data=f"Error inserting project-countries into database. {e}",
                status=500,
                safe=False
            )

class ProjectSectorBulkCreateApiView(BulkCreateApiView):
    """REST API operations for collections of project-sector pairs.
    """
    queryset = ProjectSector.objects.all()
    serializer_class = ProjectSectorSerializer
    error_message = "Error bulk inserting project-sectors into database."

