"""API views for Form 13F entities.s
"""

import json
from ..common.functions import bulk_upsert_records
from django.db.models import Q
from django.http import JsonResponse, StreamingHttpResponse
from .models import LatestForm13FInvestment
from rest_framework.views import APIView
from rest_framework.request import Request
from .serializers import (
    Form13FCompanySerializer,
    Form13FInvestmentSerializer,
    Form13FSubmissionSerializer 
)


class CompanyListApiView(APIView):
    """REST API operations for collections of companies.
    """

    def post(self, request: Request) -> JsonResponse:
        """Upserts one or more companies into the database.

        Args:
            request (`Request`): The request object.

        Returns:
            (`JsonResponse`): The list of serialized,
                newly-created companies.
        """
        try:
            # Extract records from request payload
            companies = request.data.get("records", None)

            # Return error if no companies provided
            if not companies and not isinstance(companies, list):
                return JsonResponse(
                    data="Expected to receive a list of companies.",
                    status=400,
                    safe=False)

            # Perform bulk upsert of records
            dest_tb_name = Form13FCompanySerializer.Meta.model._meta.db_table
            field_names = ["cik", "name"]
            unique_constraint_name = "unique_company_cik"
            update_table_fields = ["cik", "name"]
            upserted_companies = bulk_upsert_records(
                records=companies,
                dest_table_name=dest_tb_name,
                dest_table_fields=field_names,
                unique_constraint_name=unique_constraint_name,
                update_table_fields=update_table_fields
            )

            # Compose return payload and status
            status = 201 if upserted_companies else 200
            return JsonResponse(upserted_companies, status=status, safe=False)
        
        except Exception as e:
            return JsonResponse(
                data=f"Error upserting finalized companies into database. {e}",
                status=500,
                safe=False)

class SubmissionListApiView(APIView):
    """REST API operations for collections of Form 13F submissions.
    """

    def post(self, request: Request) -> JsonResponse:
        """Upserts one or more submissions into the database.

        Args:
            request (`Request`): The request object.

        Returns:
            (`JsonResponse`): The list of serialized,
                newly-created submissions.
        """
        try:
            # Extract records from request payload
            submissions = request.data.get("records", None)

            # Return error if no submissions provided
            if not submissions and not isinstance(submissions, list):
                return JsonResponse(
                    data="Expected to receive a list of submissions.",
                    status=400,
                    safe=False)

            # Perform bulk insert of records
            dest_tb_name = Form13FSubmissionSerializer.Meta.model._meta.db_table
            dest_table_fields = [
                "company",
                "name",
                "accession_number",
                "report_period",
                "filing_date",
                "acceptance_date",
                "effective_date",
                "url"
            ]
            constraint_name = "unique_form_accession_number"
            update_fields =  [
                "company",
                "name",
                "report_period",
                "filing_date",
                "acceptance_date",
                "effective_date",
                "url"
            ]
            upserted_submissions = bulk_upsert_records(
                records=submissions,
                dest_table_name=dest_tb_name,
                dest_table_fields=dest_table_fields,
                unique_constraint_name=constraint_name,
                update_table_fields=update_fields)

            # Compose return payload and status
            status = 201 if upserted_submissions else 200
            return JsonResponse(upserted_submissions, status=status, safe=False)
        
        except Exception as e:
            return JsonResponse(
                data=f"Error upserting Form 13F submissions into database. {e}",
                status=500,
                safe=False)

class InvestmentListApiView(APIView):
    """REST API operations for collections of Form 13F investments.
    """

    def post(self, request: Request) -> JsonResponse:
        """Inserts one or more investments into the database.

        Args:
            request (`Request`): The request object.

        Returns:
            (`JsonResponse`): The list of serialized,
                newly-created investments.
        """
        try:
            # Extract records from request payload
            investments = request.data.get("records", None)

            # Return error if no submissions provided
            if not investments and not isinstance(investments, list):
                return JsonResponse(
                    data="Expected to receive a list of investments.",
                    status=400,
                    safe=False)

            # Perform bulk insert of records
            dest_tb_name = Form13FInvestmentSerializer.Meta.model._meta.db_table
            dest_table_fields = [
                "form",
                "exchange_code",
                "issuer_name",
                "cusip",
                "title_class",
                "market_sector",
                "security_type",
                "ticker",
                "value_x1000",
                "shares_prn_amt",
                "sh_prn",
                "put_call",
                "investment_discretion",
                "manager",
                "voting_auth_sole",
                "voting_auth_shared",
                "voting_auth_none"
            ]
            constraint_name = "unique_form13f_investment"
            update_fields = ["form", "cusip", "manager"]
            upserted_investments = bulk_upsert_records(
                records=investments,
                dest_table_name=dest_tb_name,
                dest_table_fields=dest_table_fields,
                unique_constraint_name=constraint_name,
                update_table_fields=update_fields)

            # Compose return payload and status
            status = 201 if upserted_investments else 200
            return JsonResponse(upserted_investments, status=status, safe=False)
        
        except Exception as e:
            return JsonResponse(
                data=f"Error inserting Form 13F investments into database. {e}",
                status=500,
                safe=False)

class InvestmentSearchApiView(APIView):
    """REST API operations for Form 13F investmenet queries.
    """

    def post(self, request: Request) -> JsonResponse:
        """Retrieves a subset of available Form 13F
        investments using a search query.

        Args:
            request (`Request`): The Django REST Framework
                request object.

        Returns:
            (`JsonResponse`): The list of serialized
                investment records.
        """
        # Extract filters from request object
        company_cik = request.data.get("companyCik", None)
        company_name = request.data.get("companyName", None)
        accession_number = request.data.get("formAccessionNumber", None)
        report_period = request.data.get("formReportPeriod", None)
        filing_date = request.data.get("formFilingDate", None)
        acceptance_date = request.data.get("formAcceptanceDate", None)
        effective_date = request.data.get("formEffectiveDate", None)
        manager = request.data.get("manager", None)
        investment_discretion = request.data.get("investmentDiscretion", None)
        exchange_code = request.data.get("exchangeCode", None)
        issuer_name = request.data.get("issuerName", None)
        security_type = request.data.get("securityType", None)
        cusip = request.data.get("cusip", None)
        ticker = request.data.get("ticker", None)
        title_class = request.data.get("titleClass", None)
        market_sector = request.data.get("marketSector", None)
        put_call = request.data.get("putCall", None)
        search = request.data.get("search", None)

        # Extract data ordering, limit, and
        # download options from request object
        limit = request.data.get("limit", None)
        offset = request.data.get("offset", None)
        sort_col = request.data.get("sortCol", None)
        sort_desc = request.data.get("sortDesc", 0)
        is_download = request.data.get("download", 0)

        # Initialize investments to those from most recent quarter
        investments = LatestForm13FInvestment.objects

        # Filter records as indicated by request
        if company_cik:
            investments = investments.filter(
                company_cik__icontains=company_cik)

        if company_name:
            investments = investments.filter(
                company_name__icontains=company_name)

        if accession_number:
            investments = investments.filter(
                form_accession_number__icontains=accession_number)

        if report_period:
            investments = investments.filter(
                form_report_period__startswith=report_period)

        if filing_date:
            investments = investments.filter(
                form_filing_date__startswith=filing_date)

        if acceptance_date:
            investments = investments.filter(
                form_acceptance_date__startswith=acceptance_date)

        if effective_date:
            investments = investments.filter(
                form_effective_date__startswith=effective_date)

        if manager:
            investments = investments.filter(
                manager__icontains=manager)

        if investment_discretion:
            investments = investments.filter(
                investment_discretion__icontains=investment_discretion)
        
        if exchange_code:
            investments = investments.filter(
                exchange_code__icontains=exchange_code)

        if issuer_name:
            investments = investments.filter(
                issuer_name__icontains=issuer_name)

        if security_type:
            investments = investments.filter(
                security_type__icontains=security_type)
            
        if cusip:
            investments = investments.filter(cusip__icontains=cusip)

        if ticker:
            investments = investments.filter(ticker__icontains=ticker)

        if title_class:
            investments = investments.filter(
                title_class__icontains=title_class)

        if market_sector:
            investments = investments.filter(
                market_sector__icontains=market_sector)

        if put_call:
           investments = investments.filter(put_call__icontains=put_call)

        if search:
            search_query = Q()
            search_query |= Q(company_cik__icontains=search)
            search_query |= Q(company_name__icontains=search)
            search_query |= Q(form_accession_number__icontains=search)
            search_query |= Q(form_report_period__startswith=search)
            search_query |= Q(form_filing_date__startswith=search)
            search_query |= Q(form_acceptance_date__startswith=search)
            search_query |= Q(form_effective_date__startswith=search)
            search_query |= Q(manager__icontains=search)
            search_query |= Q(investment_discretion__icontains=search)
            search_query |= Q(exchange_code__icontains=search)
            search_query |= Q(issuer_name__icontains=search)
            search_query |= Q(security_type__icontains=search)
            search_query |= Q(cusip__icontains=search)
            search_query |= Q(ticker__icontains=search)
            search_query |= Q(title_class__icontains=search)
            search_query |= Q(market_sector__icontains=search)
            search_query |= Q(put_call__icontains=search)

            investments = investments.filter(search_query)

        # Order by requested column, or use company CIK by default
        default_sort_col = "companyName"
        sort_prefix = "-" if sort_desc == 1 else ""
        valid_sort_cols = {
            "companyCik": "company_cik",
            "companyName": "company_name",
            "formAccessionNumber": "form_accession_number",
            "formReportPeriod": "form_report_period",
            "formFilingDate": "form_filing_date",
            "formAcceptanceDate": "form_acceptance_date",
            "formEffectiveDate": "form_effective_date",
            "investmentDiscretion": "investment_discretion",
            "exchangeCode": "exchange_code",
            "issuerName": "issuer_name",
            "securityType": "security_type",
            "cusip": "cusip",
            "ticker": "ticker",
            "titleClass": "title_class",
            "marketSector": "market_sector",
            "numShares": "num_shares",
            "principalAmount": "principal_amount",
            "putCall": "put_call",
            "valuex1000": "value_x1000",
            "votingAuthSole": "voting_auth_sole",
            "votingAuthShared": "voting_auth_shared",
            "votingAuthNone": "voting_auth_none"
        }
        if sort_col == default_sort_col:
            investments = investments.order_by(
                f"{sort_prefix}company_name",
                "form_report_period",
                "issuer_name"
            )
        elif sort_col in valid_sort_cols.keys():
            mapped_sort_col = valid_sort_cols[sort_col]
            investments = investments.order_by(
                f"{sort_prefix}{mapped_sort_col}"
            )
        else:
            raise ValueError(f"Invalid sort column, \"{sort_col}\", received.")
    
        # Calculate total number of rows before subsetting
        total_num_rows = investments.count()

        # Validate limit parameter
        if limit:
            limit = int(limit)

        # Apply limit and, if provided, offset
        if offset and limit:
            offset = int(offset)
            investment_subset = investments[offset : offset + limit]
        else:
            investment_subset = investments[:limit]

        # Execute query 
        try:
            investment_dicts = [
                i for i in
                investment_subset.values(
                    "company_cik",
                    "company_name",
                    "form_accession_number",
                    "form_report_period",
                    "form_filing_date",
                    "form_acceptance_date",
                    "form_effective_date",
                    "form_url",
                    "manager",
                    "exchange_code",
                    "issuer_name",
                    "security_type",
                    "cusip",
                    "ticker",
                    "title_class",
                    "market_sector",
                    "value_x1000",
                    "num_shares",
                    "principal_amount",
                    "put_call",
                    "investment_discretion",
                    "voting_auth_sole",
                    "voting_auth_shared",
                    "voting_auth_none"
                )
            ]
        except Exception as e:
            return JsonResponse(
                data=f"Failed to retrieve investments from database. {e}",
                status=500,
                safe=False)

        # Return if not download request or no investments available
        if not is_download:
            payload = {
                "total_num_rows": total_num_rows,
                "investments": investment_dicts
            }
            return JsonResponse(payload, status=200, safe=False)

        # Otherwise, stream records as JSON
        try:
            def formatJson(j, idx):
                if idx == 0:
                    return "[" + json.dumps(j, default=str) + ","
                elif idx == len(investment_dicts) - 1:
                    return json.dumps(j, default=str) + "]"
                else:
                    return json.dumps(j, default=str) + ","

            return StreamingHttpResponse(
                streaming_content=(
                    (
                        formatJson(p, idx) 
                        for idx, p in 
                        enumerate(investment_dicts)
                    ) 
                    if investment_dicts else "[]"
                ),
                content_type="application/json"
            )
        except Exception as e:
            return JsonResponse(
                data=f"Failed to write investments to CSV file. {e}",
                status=500,
                safe=False
            )
