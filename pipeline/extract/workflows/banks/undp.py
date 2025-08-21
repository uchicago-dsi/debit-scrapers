"""Data extraction workflows for the United Nations
Development Programme (UNDP). Data is retrieved by
downloading a zip file containing project data and
then building URLs to project API endpoints, which
are queried to gather remaining details on financing
dates and amounts.
"""

# Standard library imports
import json
from datetime import datetime
from logging import Logger
from typing import Dict, Iterator, List, Tuple

# Third-party imports
from django.conf import settings
from lxml import etree
from stream_unzip import stream_unzip

# Application imports
from common.pubsub import PublisherClient
from common.web import DataRequestClient
from extract.dal import ExtractionDbClient
from extract.workflows.abstract import (
    ProjectPartialScrapeWorkflow,
    ResultsMultiScrapeWorkflow,
    SeedUrlsWorkflow,
)


class UndpSeedUrlsWorkflow(SeedUrlsWorkflow):
    """Generates the first set of URLs/API resources to
    query for development bank project data.
    """

    @property
    def next_workflow(self) -> str:
        """The name of the workflow to execute after this
        workflow has finished.
        """
        return settings.RESULTS_PAGE_MULTISCRAPE_WORKFLOW

    @property
    def results_page_download_url(self) -> str:
        """A link to directly download project results as a ZIP file."""
        return "https://api.open.undp.org/api/download/undp-project-data.zip"

    def generate_seed_urls(self) -> List[str]:
        """Generates the URLs used for downloading UNDP project data.

        Args:
            `None`

        Returns:
            The URLs.
        """
        return [self.results_page_download_url]


class UndpResultsMultiScrapeWorkflow(ResultsMultiScrapeWorkflow):
    """Downloads and parses a CSV file containing project URLs and data."""

    def __init__(
        self,
        data_request_client: DataRequestClient,
        pubsub_client: PublisherClient,
        db_client: ExtractionDbClient,
        logger: Logger,
    ) -> None:
        """Initializes a new instance of a `UndpResultsMultiScrapeWorkflow`.

        Args:
            data_request_client: A client for making HTTP GET requests
                while adding random delays and rotating user agent headers.

            pubsub_client: A wrapper client for the
                Google Cloud Platform Pub/Sub API. Configured to
                publish messages to the appropriate 'tasks' topic.

            db_client: A client used to insert and
                update tasks in the database.

            logger: A standard logger instance.

        Returns:
            `None`
        """
        # Initialize attributes
        super().__init__(data_request_client, pubsub_client, db_client, logger)

        # Load IATI status codes
        self._status_codes = self._load_iati_codelist(
            settings.IATI_ACTIVITY_STATUS_FPATH
        )

        # Load IATI sector codes
        self._sector_codes = self._load_iati_codelist(
            settings.IATI_ACTIVITY_SECTOR_FPATH
        )

    @property
    def project_details_api_base_url(self) -> str:
        """The base URL for an UNDP project's details, accessible via API."""
        return "https://api.open.undp.org/api/projects/{}.json"

    @property
    def project_page_base_url(self) -> str:
        """The base URL for an UNDP project page."""
        return "https://open.undp.org/projects/{}"

    def _load_iati_codelist(self, fpath: str) -> None:
        """Loads the IATI codelist from a JSON file.

        Args:
            fpath: The path to the JSON file.

        Returns:
            A dictionary mapping IATI codes to names.
        """
        with open(fpath) as f:
            loaded = json.load(f)
            return {entry["code"]: entry["name"] for entry in loaded["data"]}

    def _process_file(
        self, file: Iterator[bytes]
    ) -> Tuple[List[str], List[Dict]]:
        """Processes the given XML file containing UNDP projects and then
        scrapes the data for project records and URLs to additional
        project data accessible via API.

        Args:
            bytes: A stream of data.

        Returns:
            A tuple consisting of the project page URLs
                and partial project records.
        """
        # Load raw bytes into lxml parser
        byte_str = b"".join(file)
        root = etree.fromstring(byte_str)

        # Initialize generated URLs and extracted project records
        urls = []
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
            sectors = (
                "|".join(
                    [
                        self._sector_codes[sector.get("code")]
                        for sector in activity.findall("sector")
                        if sector.get("vocabulary") == "1"
                    ]
                )
                or None
            )

            # Parse affiliates
            affiliates = (
                "|".join(
                    set(
                        narrative.text.upper()
                        for narrative in activity.findall(
                            "participating-org/narrative"
                        )
                    )
                )
                or None
            )

            # Parse countries
            countries = (
                "|".join(
                    narrative.text
                    for narrative in activity.findall(
                        "recipient-country/narrative"
                    )
                )
                or None
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

            # Compose and append URL to project API endpoint
            urls.append(self.project_details_api_base_url.format(number))

        return urls, projects

    def scrape_results_page(self, url: str) -> Tuple[List[str], List[Dict]]:
        """Streams the ZIP file hosted at the given URL while simultaneously
        unzipping the contents and scraping partial project data and generating
        URLs to gather the remaining project information. Extracted files of
        relevance are in XML format and processed one at a time. The data
        follows the IATI (International Aid Transparency Initiative) standard,
        so mapping codes is required.

        References:
        - https://iatistandard.org/en/iati-standard/203/codelists/

        Args:
            url: The download link.

        Returns:
            A tuple consisting of the project page URLs
                and partial project records.
        """
        # Initialize generated URLs and extracted project records
        urls = []
        projects = []

        # Perform a streaming unzip of UNDP ZIP file and process each XML file
        for file_name_bytes, _, unzipped_chunks in stream_unzip(
            self._data_request_client.stream_chunks(url)
        ):
            file_name = file_name_bytes.decode("utf-8")
            if file_name.endswith("_projects.xml"):
                file_urls, file_projects = self._process_file(unzipped_chunks)
                urls.extend(file_urls)
                projects.extend(file_projects)
            else:
                for _ in unzipped_chunks:
                    pass

        return urls, projects


class UndpProjectPartialScrapeWorkflow(ProjectPartialScrapeWorkflow):
    """Retrieves project data from UNDP and saves it to a database."""

    @property
    def project_page_base_url(self) -> str:
        """The base URL for an UNDP project page."""
        return "https://open.undp.org/projects/{}"

    def scrape_project_page(self, url: str) -> List[Dict]:
        """Scrapes an UNDP project page for data.

        Args:
            url: The URL for a project.

        Returns:
            The project records.
        """
        # Fetch project data from API
        response = self._data_request_client.get(
            url, use_random_delay=True, min_random_delay=1, max_random_delay=3
        )

        # Parse project JSON
        project = response.json()

        # Parse project start date
        if project.get("start"):
            start_date_utc = datetime.strptime(
                project.get("start"), "%Y-%m-%d"
            )
        else:
            start_date_utc = None

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
