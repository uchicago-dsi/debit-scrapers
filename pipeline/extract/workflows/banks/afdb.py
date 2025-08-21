"""Data extraction workflows for the African Development Bank Group (AFDB).
Data is partially retrieved by using a headless browser to download an Excel
file of project records from the AFDB project search page. Then additional
URLs to the AFDB API (not documented) are generated and requested to gather
the remaining details for project-related organizations.
"""

# Standard library imports
import json
import os
from typing import Dict, List, Tuple

# Third-party imports
import pandas as pd
from bs4 import BeautifulSoup
from django.conf import settings
from playwright.sync_api import Page

# Application imports
from common.web import HeadlessBrowser
from extract.workflows.abstract import (
    ProjectPartialDownloadWorkflow,
    ProjectPartialScrapeWorkflow,
)


class AfdbProjectPartialDownloadWorkflow(ProjectPartialDownloadWorkflow):
    """Downloads and parses a CSV file containing project URLs and data."""

    @property
    def download_url(self) -> str:
        """The URL for the AFDB project list, where data can be downloaded."""
        return "https://mapafrica.afdb.org/en/projects"

    @property
    def project_organizations_url(self) -> str:
        """The base API URL for data on an AFDB project's organizations."""
        return "https://mapafrica.afdb.org/api/v13/activities/46002-{}/organisations"

    @property
    def project_page_url(self) -> str:
        """The base URL for an AFDB project webpage."""
        return "https://mapafrica.afdb.org/en/projects/46002-{}"

    def get_projects(self) -> pd.DataFrame:
        """Retrieves all development bank projects through a
        direct download and parses them into a Pandas DataFrame.

        Args:
            `None`

        Returns:
            The raw project records.
        """
        # Initialize headless browser
        browser = HeadlessBrowser()

        # Define function to trigger download
        def trigger(page: Page) -> None:
            """Triggers the download of project data in
            Excel format by clicking on the "XLSX" button.

            Args:
                page: The page to trigger the download on.

            Returns:
                `None`
            """
            page.get_by_role("button", name="XLSX").click()

        # Download file to temporary location on disc
        temp_file = browser.download_file(
            "https://mapafrica.afdb.org/en/projects", trigger
        )

        # Read downloaded file to Pandas DataFrame
        df = pd.read_excel(temp_file.name)

        # Destroy temp file
        os.remove(temp_file.name)

        return df

    def clean_projects(self, df: pd.DataFrame) -> Tuple[List[str], List[Dict]]:
        """Cleans project records to conform to an expected schema.

        Args:
            df: The raw project records.

        Returns:
            The cleaned records.
        """
        # Parse the project ids to build URLs to project organization pages
        urls = [
            self.project_organizations_url.format(id)
            for id in df["identifier"].tolist()
        ]

        # Add bank column to DataFrame
        df["source"] = settings.AFDB_ABBREVIATION.upper()

        # Add project URL column
        df["url"] = df["identifier"].apply(
            lambda id: self.project_page_url.format(id)
        )

        # Calculate loan amount currency
        df["total_amount_currency"] = df["total_commitments (UA)"].apply(
            lambda amount: "UA" if amount else None
        )

        # Define column mapping
        col_map = {
            "country": "countries",
            "Completion Date": "date_actual_close",
            "Approval Date": "date_approved",
            "Planned Completion Date": "date_planned_close",
            "Signature Date": "date_signed",
            "title": "name",
            "identifier": "number",
            "AfDB Sector": "sectors",
            "source": "source",
            "activity_status": "status",
            "total_commitments (UA)": "total_amount",
            "total_amount_currency": "total_amount_currency",
            "url": "url",
        }

        # Rename columns
        df = df.rename(columns=col_map)

        # Subset columns
        df = df[col_map.values()]

        return urls, df


class AfdbProjectPartialScrapeWorkflow(ProjectPartialScrapeWorkflow):
    """Requests select AFDB project data using the API."""

    @property
    def project_page_url(self) -> str:
        """The base URL for an AFDB project page."""
        return "https://mapafrica.afdb.org/en/projects/46002-{}"

    def scrape_project_page(self, url) -> List[Dict]:
        """Queries the AFDB API for details of a project's
        funders and implementers and constructs a partial
        project to update the corresponding database record.

        Args:
            url: The URL to the API resource.

        Returns:
            The raw record(s).
        """
        # Initialize headless browser
        browser = HeadlessBrowser()

        # Fetch HTML at givenURL
        html = browser.get_html(url)

        # Parse HTML into node tree
        soup = BeautifulSoup(html, "lxml")

        # Extract the serialized JSON payload
        payload = soup.find("pre").text

        # Define local function to clean organization name
        def clean(name: str) -> str:
            """Cleans an organization name.

            Args:
                name: The name to clean.

            Returns:
                The cleaned name.
            """
            for char in ["\n", "\t", "\r"]:
                name = name.replace(char, " ")
            return " ".join(name.strip().split())

        # Parse the response JSON
        try:
            orgs = "|".join(
                clean(org["organisation"]) for org in json.loads(payload)
            )
        except json.JSONDecodeError:
            raise RuntimeError(
                f'Error parsing AFDB project details at "{url}". '
                f"The response body does not contain valid JSON."
            )
        except KeyError:
            raise RuntimeError(
                f'Error parsing AFDB project details at "{url}". '
                f"The response body has an unexpected JSON schema."
            )

        # Parse URL for project identifier
        project_id = url.split("/")[-2].split("46002-")[-1]

        # Parse the response
        return {
            "affiliates": orgs,
            "source": settings.AFDB_ABBREVIATION.upper(),
            "url": self.project_page_url.format(project_id),
        }
