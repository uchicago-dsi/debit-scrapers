"""United Nations Development Programme (UNDP)

Data is retrieved by downloading a ZIP file containing project data and
then building URLs to project API endpoints, which are queried to gather
remaining details on financing dates and amounts.
"""

# Standard library imports
import json
import re
from datetime import datetime
from logging import Logger

# Third-party imports
from django.conf import settings
from lxml import etree

# Application imports
from common.http import DataRequestClient
from common.tasks import MessageQueueClient
from extract.dal import DatabaseClient
from extract.workflows.abstract import (
    ProjectPartialScrapeWorkflow,
    ResultsMultiScrapeWorkflow,
    SeedUrlsWorkflow,
)


class UndpSeedUrlsWorkflow(SeedUrlsWorkflow):
    """Retrieves the first set of UNDP URLs to scrape."""

    @property
    def iati_datasets_url(self) -> int:
        """The URL to the list of all IATI datasets."""
        return "https://bulk-data.iatistandard.org/datasets-minimal"

    @property
    def next_workflow(self) -> str:
        """The next workflow to execute."""
        return settings.RESULTS_PAGE_MULTISCRAPE_WORKFLOW

    @property
    def undp_iati_dataset_regex(self) -> str:
        """A Regex pattern to identify UNDP IATI activity dataset URLs."""
        return r"http[s]*:\/\/open\.undp\.org\/download\/iati_xml\/[A-Za-z_()'\- ]+_projects\.xml"

    def generate_seed_urls(self) -> list[str]:
        """Generates the first set of URLs to scrape.

        Args:
            `None`

        Returns:
            A list of URLs to the UNDP IATI activity datasets.
        """
        # Fetch IATI datasets
        r = self._data_request_client.get(
            url=self.iati_datasets_url,
            use_random_user_agent=True,
            use_random_delay=True,
            min_random_delay=1,
            max_random_delay=3,
        )

        # Raise error if request failed
        if not r.ok:
            raise RuntimeError(
                "Error fetching data from UNDP. "
                f"The request failed with a "
                f'"{r.status_code} - {r.reason}" status '
                f'code and the message "{r.text}".'
            )

        # Parse IATI datasets
        try:
            datasets = r.json()
        except Exception as e:
            raise RuntimeError(
                f"Error parsing UNDP IATI datasets into JSON. {e}"
            ) from None

        # Return URLs
        return [
            entry["source_url"]
            for entry in datasets["datasets"]
            if re.match(self.undp_iati_dataset_regex, entry["source_url"])
        ]


class UndpResultsMultiScrapeWorkflow(ResultsMultiScrapeWorkflow):
    """Downloads and parses a ZIP file containing project URLs and data."""

    def __init__(
        self,
        data_request_client: DataRequestClient,
        msg_queue_client: MessageQueueClient,
        db_client: DatabaseClient,
        logger: Logger,
    ) -> None:
        """Initializes a new instance of a `UndpProjectPartialDownloadWorkflow`.

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

    def scrape_results_page(self, url: str) -> tuple[list[str], list[dict]]:
        """Fetches and processes an XML file containing UNDP projects.

        Retrieves the file hosted at the given URL, scrapes partial project
        data, and generates URLs to gather the remaining project information.
        The data follows the IATI (International Aid Transparency Initiative)
        standard, so mapping codes are required.

        References:
        - https://iatistandard.org/en/iati-standard/203/codelists/

        Args:
            url: The file URL.

        Returns:
            A two-item tuple consisting of the project URLs
                and partial project records.
        """
        # Fetch XML file
        r = self._data_request_client.get(
            url,
            use_random_user_agent=True,
            use_random_delay=True,
            min_random_delay=1,
            max_random_delay=3,
        )

        # Raise error if request failed
        if not r.ok:
            raise RuntimeError(
                "Error fetching IATI dataset for UNDP. "
                f"The request failed with a "
                f'"{r.status_code} - {r.reason}" status '
                f'code and the message "{r.text}".'
            )

        # Otherwise, load raw bytes into lxml parser
        root = etree.fromstring(r.content)

        # Initialize extracted project records and next URLs to crawl
        projects = []
        next_urls = []

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
                    if narrative.text.upper()
                    not in ("UNDP", "UNITED NATIONS DEVELOPMENT PROGRAMME")
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

            # Update next URLs to crawl
            next_urls.append(self.project_details_api_base_url.format(number))

        return next_urls, projects


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
