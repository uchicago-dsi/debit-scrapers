"""Asian Development Bank (ADB)

Data is retrieved by scraping all individual project page URLs from
search result pages and then scraping details from each project page.
"""

# Standard library imports
import json
import re
from logging import Logger
from itertools import chain

# Third-party imports
import numpy as np
import pandas as pd
from django.conf import settings
from lxml import etree

# Application imports
from common.http import DataRequestClient
from extract.dal import DatabaseClient
from extract.workflows.abstract import ProjectScrapeWorkflow, SeedUrlsWorkflow


class AdbSeedUrlsWorkflow(SeedUrlsWorkflow):
    """Retrieves the first set of ADB URLs to scrape."""

    @property
    def adb_iati_dataset_search_string(self) -> str:
        """A fragment shared by all ADB IATI activity dataset URLs."""
        return "www.adb.org/iati/iati-activities-"

    @property
    def iati_datasets_url(self) -> int:
        """The URL to the list of all IATI datasets."""
        return "https://bulk-data.iatistandard.org/datasets-minimal"

    @property
    def next_workflow(self) -> str:
        """The next workflow to execute."""
        return settings.PROJECT_PAGE_WORKFLOW

    def generate_seed_urls(self) -> list[str]:
        """Generates the first set of URLs to scrape.

        Args:
            `None`

        Returns:
            A list of URLs to the ADB IATI activity datasets.
        """
        # Fetch IATI datasets
        r = self._data_request_client.get(
            url=self.iati_datasets_url,
            use_random_user_agent=True,
            use_random_delay=True,
            min_random_delay=1,
            max_random_delay=3,
        )

        # Parse IATI datasets
        try:
            datasets = r.json()
        except Exception as e:
            raise RuntimeError(
                f"Error parsing ADB IATI datasets into JSON. {e}"
            ) from None

        # Return URLs
        return [
            entry["source_url"]
            for entry in datasets["datasets"]
            if self.adb_iati_dataset_search_string in entry["source_url"]
        ]


class AdbProjectScrapeWorkflow(ProjectScrapeWorkflow):
    """Downloads an ADB IATI activity file and parses it for project data."""

    def __init__(
        self,
        data_request_client: DataRequestClient,
        db_client: DatabaseClient,
        logger: Logger,
    ) -> None:
        """Initializes a new instance of a `UndpResultsMultiScrapeWorkflow`.

        Args:
            data_request_client: A client for making HTTP GET requests
                while adding random delays and rotating user agent headers.

            db_client: A client used to insert and
                update tasks in the database.

            logger: A standard logger instance.

        Returns:
            `None`
        """
        # Initialize attributes
        super().__init__(data_request_client, db_client, logger)

        # Load IATI country codes
        self._country_codes = self._load_iati_codelist(
            settings.IATI_ACTIVITY_COUNTRY_FPATH
        )

        # Load IATI finance type codes
        self._finance_type_codes = self._load_iati_codelist(
            settings.IATI_ACTIVITY_FINANCE_TYPE_FPATH
        )

        # Load IATI sector codes
        self._sector_codes = self._load_iati_codelist(
            settings.IATI_ACTIVITY_SECTOR_FPATH
        )

        # Load IATI status codes
        self._status_codes = self._load_iati_codelist(
            settings.IATI_ACTIVITY_STATUS_FPATH
        )

    @property
    def project_page_base_url(self) -> str:
        """The base URL for an ADB project page."""
        return "https://www.adb.org/projects/{project_number}/main"

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

    def _process_activity(self, activity: etree._Element) -> dict:
        """Processes an IATI activity for project data.

        Args:
            activity: An IATI activity.

        Returns:
            The project data.
        """
        # Initialize project
        project = {}

        # Parse affiliated organizations
        project["affiliates"] = list(
            {
                narrative.text
                for narrative in activity.findall(
                    "participating-org/narrative"
                )
            }
        )

        # Parse and map affiliated countries
        countries = []
        for country in activity.findall("recipient-country"):
            country_name = self._country_codes.get(country.get("code"))
            countries.append(country_name)

        for location in activity.findall("location"):
            country_name = location.find("name/narrative").text
            countries.append(country_name)

        project["countries"] = sorted(set(countries))

        # Parse dates
        for date in activity.findall("activity-date"):
            if date.get("type") == "2":
                project["date_signed"] = date.get("iso-date")[:10]
            elif date.get("type") == "3":
                project["date_planned_close"] = date.get("iso-date")[:10]
            elif date.get("type") == "4":
                project["date_actual_close"] = date.get("iso-date")[:10]

        # Parse finance types
        finance_type_code = activity.find("default-finance-type").get("code")
        project["finance_types"] = [
            self._finance_type_codes.get(finance_type_code)
        ]

        # Parse name
        project["name"] = activity.find("title/narrative").text

        # Parse unique identifier/number
        project["number"] = "-".join(
            activity.find("iati-identifier").text.split("-")[3:5]
        )

        # Parse and map the project sectors
        project["sectors"] = [
            self._sector_codes[sector.get("code")]
            for sector in activity.findall("sector")
            if sector.get("vocabulary") == "1"
        ]

        # Set project source
        project["source"] = settings.ADB_ABBREVIATION.upper()

        # Parse status
        try:
            info_form_url = activity.find("contact-info/website").text
            match = re.search(r"[^(]+\(([^)]+)\)", info_form_url)
            project["status"] = match.group(1) if match else ""
        except Exception:
            iati_status_code = activity.find("activity-status").get("code")
            project["status"] = self._status_codes.get(iati_status_code, "")

        # Parse total amount
        for transaction in activity.findall("transaction"):
            if transaction.find("transaction-type").get("code") == "2":
                project["total_amount"] = project["total_amount_usd"] = float(
                    transaction.find("value").text
                )
                break

        # Parse currency
        project["total_amount_currency"] = activity.get("default-currency")

        # Construct URL to project page
        project["url"] = self.project_page_base_url.format(
            project_number=project["number"]
        )

        return project

    def scrape_project_page(self, url: str) -> list[dict]:
        """Extracts project details from an ADB IATI activity file.

        Args:
            url: The URL for a project.

        Returns:
            The project record(s).
        """
        # Initialize projects
        raw_projects = []

        # Stream remote file of ADB's loan activities in specific country
        with self._data_request_client.get(url, stream=True) as r:
            if not r.ok:
                raise RuntimeError(
                    "Error fetching data. The request failed with "
                    f'a "{r.status_code} - {r.reason}" status '
                    f'code and the message "{r.text}".'
                )
            r.raw.decode_content = True
            context = etree.iterparse(r.raw, events=("end",))
            for _, elem in context:
                if elem.tag == "iati-activity":
                    raw_projects.append(self._process_activity(elem))

        # Read projects into DataFrame
        df = pd.DataFrame(raw_projects)

        # Replace missing values with None
        df = df.replace({np.nan: None})

        # Define helper function for combining list-like columns
        def combine(series: pd.Series) -> str:
            """Combines values in a Pandas series into a pipe-delimited string.

            Args:
                series: The series.

            Returns:
                The string.
            """
            return "|".join(sorted(set(chain.from_iterable(series))))

        # Define helper function for parsing loan amounts
        def parse_amount(amount: np.float64) -> float | None:
            """Parses a loan amount from its Numpy representation.

            Args:
                amount: The raw loan amount.

            Returns:
                The parsed amount.
            """
            return float(amount) if not np.isnan(amount) else None

        # Define helper function for parsing the max of a list of dates
        def parse_max_date(dates: list[str | None]) -> str | None:
            """Returns the maximum of a list of dates.

            If no valid dates exist, returns an empty string.

            Args:
                dates: The list of dates.

            Returns:
                The max date.
            """
            try:
                return max(d for d in dates if d is not None)
            except ValueError:
                return ""

        # Define helper function for parsing the min of a list of dates
        def parse_min_date(dates: list[str | None]) -> str | None:
            """Returns the minimum of a list of dates.

            If no valid dates exist, returns an empty string.

            Args:
                dates: The list of dates.

            Returns:
                The min date.
            """
            try:
                return min(d for d in dates if d is not None)
            except ValueError:
                return ""

        # Define helper function for defining project status
        def parse_status(statuses: pd.Series) -> str:
            """Determines the aggregate project status.

            An ADB project consists of multiple debt instruments,
            each with their own status ("Proposed", "Approved",
            "Active", "Dropped/ Terminated", "Closed", or "Archived").

            If all the statuses are the same, the overall/aggregate project
            status should be that status. If any status is "Active", the
            project status should also be "Active" (indicating that at least
            one loan is in progress). Otherwise, the status should be "Other".

            Args:
                statuses: The statuses of the financial instruments.

            Returns:
                The aggregate project status.
            """
            if len(set(statuses)) == len(statuses):
                return statuses.iloc[0]
            elif any(s == "Active" for s in statuses):
                return "Active"
            else:
                return "Other"

        # Aggregate projects by project number/URL
        aggregated_projects = []
        for _, grp in df.groupby("url"):
            aggregated_projects.append(
                {
                    "affiliates": combine(grp["affiliates"]),
                    "countries": combine(grp["countries"]),
                    "date_signed": parse_min_date(grp["date_signed"]),
                    "date_planned_close": parse_max_date(
                        grp["date_planned_close"]
                    ),
                    "date_actual_close": parse_max_date(
                        grp["date_actual_close"]
                    ),
                    "finance_types": combine(grp["finance_types"]),
                    "name": grp["name"].iloc[0],
                    "number": grp["number"].iloc[0],
                    "sectors": combine(grp["sectors"]),
                    "source": grp["source"].iloc[0],
                    "status": parse_status(grp["status"]),
                    "total_amount": parse_amount(grp["total_amount"].sum()),
                    "total_amount_usd": parse_amount(
                        grp["total_amount_usd"].sum()
                    ),
                    "total_amount_currency": grp["total_amount_currency"].iloc[
                        0
                    ],
                    "url": grp["url"].iloc[0],
                }
            )

        return aggregated_projects
