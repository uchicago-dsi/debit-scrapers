"""African Development Bank Group (AFDB)

Data is partially retrieved by using a headless browser to download an Excel
file of project records from the AFDB project search page. Then additional
URLs to the AFDB API (not documented) are generated and requested to gather
the remaining details for project-related organizations.
"""

# Standard library imports
import json
import time
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import IO

# Third-party imports
import numpy as np
import pandas as pd
from bs4 import BeautifulSoup
from curl_cffi import requests
from django.conf import settings

# Application imports
from extract.workflows.abstract import (
    ProjectPartialDownloadWorkflow,
    ProjectPartialScrapeWorkflow,
)


class AfdbProjectPartialDownloadWorkflow(ProjectPartialDownloadWorkflow):
    """Downloads and parses a CSV file containing project URLs and data."""

    @property
    def download_url(self) -> str:
        """The URL for the AFDB data download trigger."""
        return "https://mapafrica.afdb.org/api/v14/downloads/download?data_format=xlsx&lang=en&currency=xdr"

    @property
    def project_organizations_url(self) -> str:
        """The base API URL for data on an AFDB project's organizations."""
        return "https://mapafrica.afdb.org/api/v13/activities/46002-{}/organisations"

    @property
    def project_page_url(self) -> str:
        """The base URL for an AFDB project webpage."""
        return "https://mapafrica.afdb.org/en/projects/46002-{}"

    def _trigger_download(self, session: requests.Session) -> str:
        """Triggers a download of project data from the AFDB website.

        Raises:
            RuntimeError: If the download trigger fails or
                its response body cannot be parsed.

        Args:
            session: A requests session.

        Returns:
            The download id.
        """
        # Trigger download
        r = session.get(
            self.download_url,
            impersonate="chrome110",
            timeout=60,
        )

        # Raise exception if error occurred
        if not r.status_code:
            raise RuntimeError(
                "Error downloading data. The request to trigger "
                "the data download failed with a status code of "
                f'"{r.status_code} - {r.reason}".'
            )

        # Parse download id from response payload
        try:
            return r.json()["download_id"]
        except (json.JSONDecodeError, KeyError):
            raise RuntimeError(
                "Error downloading data. The request to "
                "trigger the data download did not return "
                f"the expected response payload: {r.text}."
            ) from None

    def _wait_for_download(
        self,
        session: requests.Session,
        download_id: str,
        max_wait_in_seconds: int = 180,
        check_interval_in_seconds: int = 10,
    ) -> str:
        """Waits for a download to complete.

        Raises:
            RuntimeError: If the download fails, is not completed
                after the maximum number of checks has been reached,
                or has a response body that cannot be parsed.

        Args:
            session: A requests session.

            download_id: The download id.

            max_wait_in_seconds: The maximum time to wait for the
                download to complete. Defaults to 180 seconds.

            check_interval_in_seconds: The delay between status
                checks in seconds. Defaults to 10.

        Returns:
            The name of the file to download.
        """
        # Wait until download is complete
        elapsed_in_seconds = 0
        while True:
            # Check download status
            r = session.get(
                f"https://mapafrica.afdb.org/api/v14/downloads/download/{download_id}",
                impersonate="chrome110",
                timeout=60,
            )

            # Raise exception if error occurred
            if not r.status_code:
                raise RuntimeError(
                    "Error downloading data. The request to "
                    "check the status of the data download "
                    f"failed with a status code of "
                    f'"{r.status_code} - {r.reason}".'
                )

            # Parse response body
            try:
                payload = r.json()
                state = payload["state"]
                if state == "SUCCESS":
                    return payload["file"]
                if state == "FAILURE":
                    raise RuntimeError(
                        "Error downloading data. AFDB reported a failed "
                        f"download state for id '{download_id}': {payload}."
                    )
            except (json.JSONDecodeError, KeyError):
                raise RuntimeError(
                    "Error downloading data. The request to "
                    "check the status of the data download "
                    f"did not return the expected response "
                    f"payload: {r.text}."
                ) from None

            # Wait before checking status again
            time.sleep(check_interval_in_seconds)
            elapsed_in_seconds += check_interval_in_seconds

            # Raise exception if download has not completed
            if elapsed_in_seconds >= max_wait_in_seconds:
                raise RuntimeError(
                    "Error downloading data. The data download has not "
                    f"completed after {max_wait_in_seconds} seconds."
                )

    def _download_file(self, session: requests.Session, file_name: str) -> IO[bytes]:
        """Downloads a file from the AFDB website.

        Raises:
            RuntimeError: If the download fails.

        Args:
            session: A requests session.

            file_name: The name of the file to download

        Returns:
            The file contents.
        """
        # Fetch file contents
        r = session.get(
            f"https://mapafrica.afdb.org/api/downloads/{file_name}",
            impersonate="chrome110",
            timeout=60,
        )

        # Raise exception if error occurred
        if not r.status_code:
            raise RuntimeError(
                "Error downloading data. The request to "
                "download the data file failed with a "
                f'status code of "{r.status_code} - {r.reason}".'
            )

        # Otherwise, create temporary file
        prefix, suffix = file_name.split(".")
        temp_file = NamedTemporaryFile(delete=False, prefix=prefix, suffix=f".{suffix}")

        # Close file handle to make writable
        temp_file.close()

        # Save download to temporary file
        with open(temp_file.name, "wb") as f:
            f.write(r.content)

        # Return temporary file
        return temp_file

    def _fetch_projects_from_activities_api(self) -> pd.DataFrame:
        """Fetches project data from AFDB's activities API as a native fallback.

        Returns:
            A DataFrame with the same source columns expected by `clean_projects`.
        """
        session = requests.Session()
        rows = []

        for page in range(1, 500):
            r = session.get(
                f"https://mapafrica.afdb.org/api/v13/activities?page={page}",
                impersonate="chrome110",
                timeout=60,
            )
            if not r.ok:
                raise RuntimeError(
                    "Error downloading AFDB activities API data. "
                    f'Request failed with "{r.status_code} - {r.reason}".'
                )

            try:
                results = r.json()["results"]
            except (json.JSONDecodeError, KeyError):
                raise RuntimeError(
                    "Error parsing AFDB activities API response body."
                ) from None

            if not results:
                break

            for row in results:
                iati_identifier = row.get("iati_identifier", "")
                identifier = iati_identifier.removeprefix("46002-")
                rows.append(
                    {
                        "identifier": identifier,
                        "country": row.get("country", ""),
                        "Completion Date": row.get("activity_date_actual_end", ""),
                        "Approval Date": row.get("activity_date_actual_start", ""),
                        "Planned Completion Date": row.get(
                            "activity_date_planned_end", ""
                        ),
                        "Signature Date": "",
                        "title": row.get("title", ""),
                        "AfDB Sector": row.get("sector_en", ""),
                        "activity_status": row.get("activity_status", ""),
                        "total_commitments (UA)": row.get("total_commitments", ""),
                    }
                )

        if not rows:
            raise RuntimeError(
                "Error downloading AFDB activities API data. No records found."
            )

        return pd.DataFrame(rows)

    def get_projects(self) -> pd.DataFrame:
        """Downloads project records from an Excel file hosted on the website.

        Args:
            `None`

        Returns:
            The raw project records.
        """
        max_attempts = 3
        last_error = None
        file_name = None
        for attempt in range(max_attempts):
            # Initialize new session
            session = requests.Session()
            try:
                # Trigger download
                download_id = self._trigger_download(session)

                # Wait for download to complete
                file_name = self._wait_for_download(session, download_id)
                break
            except RuntimeError as e:
                last_error = e
                if attempt == max_attempts - 1:
                    break
                time.sleep(3)

        # Fall back to the AFDB activities API if file downloads fail.
        if not file_name:
            return self._fetch_projects_from_activities_api()

        # Download file
        temp_file = self._download_file(session, file_name)

        # Read downloaded file to Pandas DataFrame
        df = pd.read_excel(temp_file.name)

        # Destroy temp file
        Path(temp_file.name).unlink()

        if df.empty and last_error:
            raise last_error
        return df

    def clean_projects(self, df: pd.DataFrame) -> tuple[list[str], pd.DataFrame]:
        """Cleans project records and parses the next set of URLs to crawl.

        Args:
            df: The raw project records.

        Returns:
            A two-item tuple consisting of the new URLs and cleaned records.
        """
        # Parse the project ids to build URLs to project organization pages
        urls = [
            self.project_organizations_url.format(id)
            for id in df["identifier"].tolist()
        ]

        # Add bank column to DataFrame
        df["source"] = settings.AFDB_ABBREVIATION.upper()

        # Add project URL column
        df["url"] = df["identifier"].apply(lambda id: self.project_page_url.format(id))

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

        # Replace NaN values with empty strings
        df = df.replace({np.nan: ""})

        # Replace empty strings with None in numeric columns
        df["total_amount"] = df["total_amount"].replace({"": None})

        # Subset columns
        df = df[col_map.values()]

        return urls, df


class AfdbProjectPartialScrapeWorkflow(ProjectPartialScrapeWorkflow):
    """Requests select AFDB project data using the API."""

    @property
    def project_page_base_url(self) -> str:
        """The base URL for an AFDB project page."""
        return "https://mapafrica.afdb.org/en/projects/46002-{}"

    def scrape_project_page(self, url: str) -> list[dict]:
        """Extracts project funders and implementers from an AFDB API payload.

        Args:
            url: The URL to the API resource.

        Returns:
            The partial project record(s).
        """
        from common.browser import HeadlessBrowser

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
            orgs = "|".join(clean(org["organisation"]) for org in json.loads(payload))
        except json.JSONDecodeError:
            raise RuntimeError(
                f'Error parsing AFDB project details at "{url}". '
                f"The response body does not contain valid JSON."
            ) from None
        except KeyError:
            raise RuntimeError(
                f'Error parsing AFDB project details at "{url}". '
                f"The response body has an unexpected JSON schema."
            ) from None

        # Parse URL for project identifier
        project_id = url.split("/")[-2].split("46002-")[-1]

        # Parse the response
        return [
            {
                "affiliates": orgs,
                "source": settings.AFDB_ABBREVIATION.upper(),
                "url": self.project_page_base_url.format(project_id),
            }
        ]
