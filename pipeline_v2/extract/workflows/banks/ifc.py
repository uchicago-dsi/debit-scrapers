"""Data extraction workflow for the International Finance
Corporation (IFC). Data currently retrieved by querying an
external API for lists of project records.
"""

# Standard library imports
import json
import re
from typing import Dict, List

# Third-party imports
import numpy as np
import pandas as pd
from django.conf import settings

# Application imports
from extract.workflows.abstract import ProjectScrapeWorkflow, SeedUrlsWorkflow


class IfcSeedUrlsWorkflow(SeedUrlsWorkflow):
    """Retrieves the first set of IFC URLs to download."""

    @property
    def next_workflow(self) -> str:
        """The name of the workflow to execute after this
        workflow has finished.
        """
        return settings.PROJECT_PAGE_WORKFLOW

    @property
    def num_projects_per_download(self) -> int:
        """The number of projects to download from the API at once."""
        return 100

    @property
    def search_api_base_url(self) -> str:
        """The base URL for the IFC development project search API."""
        return "https://disclosuresservice.ifc.org/api/searchprovider/searchenterpriseprojects"

    @property
    def search_api_query_string(self) -> str:
        """The query string to use when paginating through IFC
        development projects exposed through the search API.
        Should be formatted with an offset value and number of rows.
        """
        return "*&$start={}$srt=disclosed_date$order=desc$rows={}"

    @property
    def start_offset(self) -> int:
        """The starting index to use when downloading project records."""
        return 0

    def generate_seed_urls(self) -> List[str]:
        """Generates the URLs used for downloading IFC project data.

        Args:
            `None`

        Returns:
            The download URLs.
        """
        try:
            # Determine number of downloads necessary to retrieve
            # all development project data
            num_projects = self.get_num_projects()
            num_download_batches = (
                num_projects // self.num_projects_per_download
            ) + (1 if num_projects % self.num_projects_per_download > 0 else 0)

            # Generate download URLs, specifying the number of
            # projects that can be obtained from IFC at once
            start = self.start_offset
            end = num_download_batches * self.num_projects_per_download
            increment = nrows = self.num_projects_per_download

            download_urls = []
            for offset in range(start, end, increment):
                query_str = self.search_api_query_string.format(offset, nrows)
                download_urls.append(
                    f"{self.search_api_base_url}?payload={query_str}"
                )

            return download_urls

        except Exception as e:
            raise RuntimeError(
                f"Failed to generate IFC project download URLs. {e}"
            )

    def get_num_projects(self) -> int:
        """Retrieves the total number of development bank
        projects available on the site, as given by the
        search results page.

        Args:
            None

        Returns:
            The search result count.
        """
        # Make IFC search results page request
        r = self._data_request_client.post(
            url=self.search_api_base_url,
            json={
                "projectNumberSearch": self.search_api_query_string.format(
                    self.start_offset, self.num_projects_per_download
                )
            },
            use_random_user_agent=True,
        )

        # Raise error if request fails
        if not r.ok:
            raise RuntimeError(
                "Error fetching IFC search results page. "
                f"The request failed with a "
                f'"{r.status_code} - {r.reason}" status '
                f'code and the message "{r.text}".'
            )

        # Parse JSON response to retrieve total number of projects
        try:
            r = r.json()
            results_metadata = r["SearchResult"]["data"]["results"]["header"]
            num_results = int(results_metadata["listInfo"]["totalRows"])
        except json.JSONDecodeError:
            raise RuntimeError(
                "Error parsing IFC project search results into JSON."
            )
        except (KeyError, TypeError):
            raise RuntimeError(
                "The IFC project search results had an unexpected JSON schema."
            )

        return num_results


class IfcProjectScrapeWorkflow(ProjectScrapeWorkflow):
    """Queries project records from IFC's API and then
    cleans and saves the data to a database using
    the `execute` method defined in its superclass.
    """

    @property
    def project_detail_base_url(self) -> str:
        """The base URL for individual project pages."""
        return "https://disclosures.ifc.org/project-detail"

    @property
    def search_api_base_url(self) -> str:
        """The base URL for the IFC development project search API."""
        return "https://disclosuresservice.ifc.org/api/searchprovider/searchenterpriseprojects"

    def scrape_project_page(self, url) -> List[Dict]:
        """Queries an IFC endpoint for development project
        records in Excel/CSV format and reads those
        records into a Pandas DataFrame. Then maps the
        records to the expected output format.

        Args:
            url: The project download URL.

        Returns:
            The project records.
        """
        # Fetch page of project records
        payload = url.split("?payload=")[1]
        r = self._data_request_client.post(
            url=self.search_api_base_url,
            json={"projectNumberSearch": payload},
            use_random_user_agent=True,
            timeout_in_seconds=None,
        )

        # Raise error if request fails
        if not r.ok:
            raise RuntimeError(
                "Error fetching IFC project records. "
                f"The request failed with a "
                f'"{r.status_code} - {r.reason}" status '
                f'code and the message "{r.text}".'
            )

        # Extract records from response body JSON
        try:
            projects = [
                data["basicinfo"]
                for data in r.json()["SearchResult"]["data"]["results"]["data"]
            ]
        except json.JSONDecodeError:
            raise RuntimeError("Error parsing IFC project records into JSON.")
        except KeyError:
            raise RuntimeError(
                "The IFC project records had an unexpected JSON schema."
            )

        # Read records into Pandas DataFrame
        df = pd.DataFrame(projects)

        # Filter records to keep investments only
        doc_type_mapping = {
            "Early Disclosure": "ED",
            "Summary of InfraVentures Project": "SIVP",
            "Summary of Investment Information (AIP Policy 2012)": "SII",
            "Summary of Proposed Investment (Disclosure Policy 2006)": "SPI",
        }
        df = df[df["document_type_description"].isin(doc_type_mapping.keys())]

        # Return empty list if no projects remaining after filter
        if len(df) == 0:
            return []

        # Otherwise, standardize select column names
        col_mapping = {
            "project_id": "id",
            "project_number": "number",
            "project_name": "name",
            "status_description": "status",
            "investment": "total_amount",
            "industry_description": "sectors",
            "country_description": "countries",
            "company_name": "affiliates",
            "approval_date": "approval_date",
            "disclosed_date": "disclosed_date",
            "estimated_start_date": "estimated_start_date",
        }
        for key in col_mapping.keys():
            if key not in df.columns:
                df[key] = None
        df = df.rename(columns=col_mapping)

        # Set bank name
        df["source"] = settings.IFC_ABBREVIATION.upper()

        # Correct country names
        def correct_country_name(name: str) -> str:
            """Rearranges a formal country name to remove
            its comma (e.g., "China, People's Republic
            of" becomes "People's Republic of China").
            At the time of writing, only one country
            is listed per project record for IFC, so
            combining different countries into one string
            is not a concern.

            Args:
                name: The country name.

            Returns:
                The formatted name.
            """
            if not name or name is np.nan:
                return None

            name_parts = name.split(",")
            uses_formal_name = len(name_parts) == 2
            if uses_formal_name:
                return f"{name_parts[1].strip()} {name_parts[0]}"

            return name

        df.loc[:, "countries"] = df["countries"].apply(correct_country_name)

        # Extract investment amount value and currency type
        def get_multiplier(amount: str | float) -> float | None:
            """Returns the multiplier for investment amounts.

            Args:
                amount: The investment amount.

            Returns:
                The multiplier.
            """
            if isinstance(amount, float) or not amount:
                return None
            elif "million" in amount.lower():
                return 10**6
            elif "billion" in amount.lower():
                return 10**9
            else:
                return 1

        df["multiplier"] = df["total_amount"].apply(get_multiplier)
        df["total_amount_currency"] = df["total_amount"].str.extract(
            r"\((.*?)\)"
        )
        df["total_amount"] = df["total_amount"].str.extract("(.*?) million")
        df["total_amount"] = pd.to_numeric(df["total_amount"], errors="coerce")
        df["total_amount"] = df["total_amount"] * df["multiplier"]
        df["total_amount_usd"] = df.apply(
            lambda row: (
                row["total_amount"]
                if row["total_amount_currency"] == "USD"
                else None
            ),
            axis=1,
        )

        # Parse date columns
        def parse_date(val: str) -> str:
            """Parses a date column into a string formatted as YYYY-MM-DD.

            Args:
                date_col: The date column.

            Returns:
                The parsed date.
            """
            if not val or val is np.nan:
                return None
            return pd.to_datetime(val).strftime("%Y-%m-%d")

        df["date_approved"] = df["approval_date"].apply(parse_date)
        df["date_disclosed"] = df["disclosed_date"].apply(parse_date)
        df["date_planned_effective"] = df["estimated_start_date"].apply(
            parse_date
        )

        # Build project URLs using project name, number, and document type
        def generate_project_detail_url(row: pd.Series) -> str:
            """Builds a complete URL to an IFC project detail page.

            Args:
                row: The DataFrame row.

            Returns:
                The URL.
            """
            # Compose URL fragment containing project name
            regex = '[()"#/@;:<>{}`+=~|.!?,]'
            substitute = (
                row["name"].lower().replace(" ", "-").replace("---", "-")
            )
            proj_name_url_frag = re.sub(regex, "", substitute)

            # Parse other needed fields into str types
            doc_type = str(row["doc_type"])
            proj_num = str(row["number"])

            return f"{self.project_detail_base_url}/{doc_type}/{proj_num}/{proj_name_url_frag}"

        df["doc_type"] = df["document_type_description"].map(doc_type_mapping)
        df["url"] = df.apply(generate_project_detail_url, axis=1)

        # Set final column schema
        cols_to_keep = [
            "affiliates",
            "countries",
            "date_approved",
            "date_disclosed",
            "date_planned_effective",
            "name",
            "number",
            "sectors",
            "source",
            "status",
            "total_amount",
            "total_amount_currency",
            "total_amount_usd",
            "url",
        ]
        df = df[cols_to_keep]

        # Replace NaN values
        df = df.replace({np.nan: None})

        return df.to_dict(orient="records")
