"""United Nations Development Programme (UNDP)

Data is retrieved by downloading a zip file containing project data and
then building URLs to project API endpoints, which are queried to gather
remaining details on financing dates and amounts.
"""

# Standard library imports
import json
from collections.abc import Iterator
from datetime import datetime
from logging import Logger

# Third-party imports
import pandas as pd
from django.conf import settings
from lxml import etree
from stream_unzip import stream_unzip

# Application imports
from common.http import DataRequestClient
from common.tasks import MessageQueueClient
from extract.dal import DatabaseClient
from extract.workflows.abstract import (
    ProjectPartialDownloadWorkflow,
    ProjectPartialScrapeWorkflow,
)


class UndpProjectPartialDownloadWorkflow(ProjectPartialDownloadWorkflow):
    """Downloads and parses a ZIP file containing project URLs and data."""

    def __init__(
        self,
        data_request_client: DataRequestClient,
        msg_queue_client: MessageQueueClient,
        db_client: DatabaseClient,
        logger: Logger,
    ) -> None:
        """Initializes a new instance of a `UndpResultsMultiScrapeWorkflow`.

        Args:
            data_request_client: A client for making HTTP GET requests
                while adding random delays and rotating user agent headers.

            msg_queue_client: A client for a message queue.

            db_client: A client used to insert and
                update tasks in the database.

            logger: A standard logger instance.

        Returns:
            `None`
        """
        # Initialize attributes
        super().__init__(
            data_request_client, msg_queue_client, db_client, logger
        )

        # Load IATI status codes
        self._status_codes = self._load_iati_codelist(
            settings.IATI_ACTIVITY_STATUS_FPATH
        )

        # Load IATI sector codes
        self._sector_codes = self._load_iati_codelist(
            settings.IATI_ACTIVITY_SECTOR_FPATH
        )

    @property
    def download_url(self) -> str:
        """A link to directly download project results as a ZIP file."""
        return "https://api.open.undp.org/api/download/undp-project-data.zip"

    @property
    def project_details_api_base_url(self) -> str:
        """The base URL for an UNDP project's details, accessible via API."""
        return "https://api.open.undp.org/api/projects/{}.json"

    @property
    def project_page_base_url(self) -> str:
        """The base URL for an UNDP project page."""
        return "https://open.undp.org/projects/{}"

    def _load_iati_codelist(self, fpath: str) -> dict:
        """Loads the IATI codelist from a JSON file.

        Args:
            fpath: The path to the JSON file.

        Returns:
            A dictionary mapping IATI codes to names.
        """
        with open(fpath) as f:
            loaded = json.load(f)
            return {entry["code"]: entry["name"] for entry in loaded["data"]}

    def _process_file(self, file: Iterator[bytes]) -> list[dict]:
        """Parses an IATI file for project records.

        Args:
            file: A stream of data.

        Returns:
            The partial project records.
        """
        # Load raw bytes into lxml parser
        byte_str = b"".join(file)
        root = etree.fromstring(byte_str)

        # Initialize extracted project records
        projects = []

        # Process each development project activity within file
        for activity in root.getchildren():
            # Skip child node if not a project activity
            if (
                activity.tag != "iati-activity"
                or "project"
                not in activity.find("iati-identifier").text.lower()
            ):
                continue

            # Otherwise, parse the project number
            number = activity.find("iati-identifier").text.split("-")[-1]

            # Parse the project name
            name = activity.find("title/narrative").text

            # Parse and map the project status code
            status = self._status_codes[
                activity.find("activity-status").get("code")
            ]

            # Parse and map the project sectors
            sectors = "|".join(
                [
                    self._sector_codes[sector.get("code")]
                    for sector in activity.findall("sector")
                    if sector.get("vocabulary") == "1"
                ]
            )

            # Parse affiliates
            affiliates = "|".join(
                {
                    narrative.text.upper()
                    for narrative in activity.findall(
                        "participating-org/narrative"
                    )
                }
            )

            # Parse countries
            countries = "|".join(
                narrative.text
                for narrative in activity.findall(
                    "recipient-country/narrative"
                )
            )

            # Build URL to project webpage
            url = self.project_page_base_url.format(number)

            # Compose and append partial project record
            projects.append(
                {
                    "affiliates": affiliates,
                    "countries": countries,
                    "name": name,
                    "number": number,
                    "sectors": sectors,
                    "source": settings.UNDP_ABBREVIATION.upper(),
                    "status": status,
                    "url": url,
                }
            )

        return projects

    def get_projects(self) -> pd.DataFrame:
        """Fetches and processes a zipped IATI file containing UNDP projects.

        Streams the ZIP file hosted at the given URL while simultaneously
        unzipping the contents, scraping partial project data, and generating
        URLs to gather the remaining project information. Extracted files of
        relevance are in XML format and processed one at a time. The data
        follows the IATI (International Aid Transparency Initiative) standard,
        so mapping codes are required.

        References:
        - https://iatistandard.org/en/iati-standard/203/codelists/

        Args:
            url: The download link.

        Returns:
            The partial project records.
        """
        # Initialize generated URLs and extracted project records
        projects = []

        # Perform a streaming unzip of UNDP ZIP file and process each XML file
        for file_name_bytes, _, unzipped_chunks in stream_unzip(
            self._data_request_client.stream_chunks(self.download_url)
        ):
            file_name = file_name_bytes.decode("utf-8")
            if file_name.endswith("_projects.xml"):
                file_projects = self._process_file(unzipped_chunks)
                projects.extend(file_projects)
            else:
                for _ in unzipped_chunks:
                    pass

        return pd.DataFrame(projects)

    def clean_projects(
        self, df: pd.DataFrame
    ) -> tuple[list[str], pd.DataFrame]:
        """Cleans project records and parses the next set of URLs to crawl.

        Args:
            df: The raw project records.

        Returns:
            A two-item tuple consisting of the new URLs and cleaned records.
        """
        urls = [
            self.project_details_api_base_url.format(number)
            for number in df["number"].tolist()
        ]
        return urls, df


class UndpProjectPartialScrapeWorkflow(ProjectPartialScrapeWorkflow):
    """Scrapes a UNDP API payload for development bank project data."""

    @property
    def project_page_base_url(self) -> str:
        """The base URL for an UNDP project page."""
        return "https://open.undp.org/projects/{}"

    def scrape_project_page(self, url: str) -> list[dict]:
        """Scrapes an UNDP project page for data.

        Args:
            url: The URL for a project.

        Returns:
            The project records.
        """
        # Fetch project data from API
        response = self._data_request_client.get(
            url, use_random_user_agent=True, use_random_delay=True
        )

        # Parse project JSON
        project = response.json()

        # Parse project start date
        if project.get("start"):
            start_date_utc = datetime.strptime(
                project.get("start"), "%Y-%m-%d"
            )
        else:
            start_date_utc = ""

        # Parse project number
        number = project["project_id"]

        # Parse project budget
        budget = project["budget"]

        # Compose final project record schema
        return [
            {
                "date_effective": start_date_utc,
                "source": settings.UNDP_ABBREVIATION.upper(),
                "total_amount": budget,
                "total_amount_currency": "USD",
                "total_amount_usd": budget,
                "url": self.project_page_base_url.format(number),
            }
        ]
