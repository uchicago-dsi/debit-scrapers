"""European Investment Bank (EIB)

Data is retrieved by scraping project search result API payloads for project
data and links to individual project webpages. The webpages are then
scraped themselves for information on project promoters and intermediaries.
"""

# Standard library imports
import json
from datetime import datetime

# Third-party imports
import numpy as np
from bs4 import BeautifulSoup
from django.conf import settings

# Application imports
from extract.workflows.abstract import (
    ProjectPartialScrapeWorkflow,
    ResultsMultiScrapeWorkflow,
    SeedUrlsWorkflow,
)


class EibSeedUrlsWorkflow(SeedUrlsWorkflow):
    """Retrieves the first set of EIB URLs to scrape."""

    @property
    def first_page_num(self) -> str:
        """The starting page number for development project search results."""
        return 0

    @property
    def next_workflow(self) -> str:
        """The next workflow to execute."""
        return settings.RESULTS_PAGE_MULTISCRAPE_WORKFLOW

    @property
    def num_results_per_page(self) -> int:
        """The number of search result items to return per page."""
        return 100

    @property
    def search_results_base_url(self) -> str:
        """The base URL for a search results payload provided by EIB's API."""
        return "https://www.eib.org/page-provider/projects/list?pageNumber={page_num}&itemPerPage={items_per_page}&pageable=true&sortColumn=id"

    def _find_last_page(self) -> int:
        """Retrieves the number of the last search results page.

        Args:
            `None`

        Returns:
            The page number.
        """
        try:
            first_results_page_url = self.search_results_base_url.format(
                page_num=self.first_page_num,
                items_per_page=self.num_results_per_page,
            )
            r = self._data_request_client.get(
                first_results_page_url,
                use_random_user_agent=True,
                use_random_delay=True,
            )
            data = r.json()
            total_num_items = int(data["totalItems"])
            return (total_num_items // self.num_results_per_page) + (
                1 if total_num_items % self.num_results_per_page > 0 else 0
            )
        except Exception as e:
            raise RuntimeError(
                "Error determining last page number from API "
                f'payload retrieved from "{first_results_page_url}". {e}'
            ) from None

    def generate_seed_urls(self) -> list[str]:
        """Generates the first set of URLs to scrape.

        Args:
            `None`

        Returns:
            The unique list of search result pages.
        """
        try:
            last_page_num = self._find_last_page()
            result_page_urls = [
                self.search_results_base_url.format(
                    page_num=n, items_per_page=self.num_results_per_page
                )
                for n in range(self.first_page_num, last_page_num + 1)
            ]
            return result_page_urls
        except Exception as e:
            raise RuntimeError(
                f"Failed to generate search result pages to crawl. {e}"
            ) from None


class EibResultsMultiScrapeWorkflow(ResultsMultiScrapeWorkflow):
    """Scrapes an EIB search results payload for both project data and URLs."""

    @property
    def project_base_url(self) -> str:
        """The base URL for an EIB project webpage."""
        return "https://www.eib.org/en/projects/all/{project_id}"

    def _map_project_record(self, project: dict) -> dict:
        """Maps an EIB project record to an expected schema.

        Args:
            project: The project record retrieved from the API.

        Returns:
            The mapped project record.
        """

        # Create local function to correct country names
        def correct_country_name(name: str) -> str:
            """Formats a country name.

            Rearranges a formal country name to remove
            its comma (e.g., "China, People's Republic
            of" becomes "People's Republic of China").
            At the time of writing, only one country
            is listed per project record for ADB, so
            combining different countries into one string
            is not a concern.

            Args:
                name: The country name.

            Returns:
                The formatted name.
            """
            if not name or name is np.nan:
                return ""

            name_parts = name.split(",")
            num_formal_name_parts = 2
            uses_formal_name = len(name_parts) == num_formal_name_parts
            if uses_formal_name:
                return f"{name_parts[1].strip()} {name_parts[0]}"

            return name

        # Extract and format project countries and sectors
        countries = []
        sectors = []
        for tag in project["primaryTags"]:
            if tag["subType"] == "countries":
                corrected_name = correct_country_name(tag["label"])
                countries.append(corrected_name)
            if tag["subType"] == "sectors":
                sectors.append(tag["label"])

        # Extract project status and loan amount data from additional information section
        status, status_date, proposed_amt, financed_amt = project[
            "additionalInformation"
        ]

        # Determine status date
        if status_date:
            parsed_status_date = datetime.strptime(status_date, "%d/%m/%Y")
            formatted_status_date = parsed_status_date.strftime("%Y-%m-%d")
            under_appraisal_utc = (
                formatted_status_date if status == "Under appraisal" else ""
            )
            approved_utc = (
                formatted_status_date if status == "Approved" else ""
            )
            signed_utc = (
                formatted_status_date
                if status not in ("Under appraisal", "Approved")
                else ""
            )
        else:
            under_appraisal_utc = approved_utc = signed_utc = ""

        # Determine loan amount
        proposed_amt = float(proposed_amt) if proposed_amt else None
        financed_amt = float(financed_amt) if financed_amt else None
        amount = (
            proposed_amt
            if status in ("Approved", "Under appraisal")
            else financed_amt
        )

        # Determine project url
        url = self.project_base_url.format(project_id=project["url"])

        return {
            "countries": "|".join(countries) if countries else "",
            "date_approved": approved_utc,
            "date_under_appraisal": under_appraisal_utc,
            "date_signed": signed_utc,
            "name": project["title"],
            "number": project["id"],
            "source": settings.EIB_ABBREVIATION.upper(),
            "status": status,
            "total_amount": amount,
            "total_amount_currency": "EUR",
            "sectors": "|".join(sectors),
            "url": url,
        }

    def scrape_results_page(self, url: str) -> tuple[list[str], list[dict]]:
        """Scrapes a search result payload for project data and webpage URLs.

        Args:
            url: The URL for the results payload.

        Returns:
            The raw record(s).
        """
        # Fetch project data
        r = self._data_request_client.get(
            url, use_random_user_agent=True, use_random_delay=True
        )

        # Raise error if request failed
        if not r.ok:
            raise RuntimeError(
                "Error fetching data from EIB. "
                f"The request failed with a "
                f'"{r.status_code} - {r.reason}" status '
                f'code and the message "{r.text}".'
            )

        # Otherwise, parse JSON response
        try:
            projects = r.json()
        except json.JSONDecodeError:
            raise RuntimeError(
                "Error parsing EIB projects into JSON."
            ) from None

        # Map project records and extract URLs
        try:
            records = [self._map_project_record(p) for p in projects["data"]]
            urls = [r["url"] for r in records]
        except Exception as e:
            raise RuntimeError(
                f"Failed to parse EIB projects from '{url}'. {e}"
            ) from None

        return urls, records


class EibProjectPartialScrapeWorkflow(ProjectPartialScrapeWorkflow):
    """Scrapes an EIB project webpage for development bank project data."""

    def scrape_project_page(self, url: str) -> list[dict]:
        """Scrapes a project webpage for data.

        NOTE: EIB has several hundred broken project links in its search
        results; consequently, some requests for project webpages may
        fail with a "404 - Not Found" status code. These errors are
        gracefully handled by returning an empty list of project records.

        Args:
            url: The URL for the project page.

        Returns:
            The project record, within a list.
        """
        # Fetch webpage
        r = self._data_request_client.get(
            url, use_random_user_agent=True, use_random_delay=True
        )

        # Return empty list if request failed
        if r.status_code == 404:  # noqa
            return []

        # Raise error if request failed for any other reason
        if not r.ok:
            raise RuntimeError(
                "Error fetching project page from EIB "
                f'at "{url}". The request failed with a '
                f'"{r.status_code} - {r.reason}" status '
                f'code and the message "{r.text}".'
            )

        # Otherwise, parse webpage HTML into node tree
        soup = BeautifulSoup(r.text, "html.parser")

        # Scrape project promoters and financial intermediaries
        try:
            col_label_div = soup.find(
                "div", string="Promoter - financial intermediary"
            )
            header_row_div = col_label_div.find_parent(
                "div", class_="eib-list__row eib-list__row--header"
            )
            body_row_div = header_row_div.find_next_sibling(
                "div", class_="eib-list__row eib-list__row--body"
            )
            promoter_col = body_row_div.find_all(
                "div", class_="eib-list__column"
            )[1]
            companies = promoter_col.text.strip()
        except (AttributeError, TypeError):
            companies = ""

        return [
            {
                "affiliates": companies,
                "source": settings.EIB_ABBREVIATION.upper(),
                "url": url,
            }
        ]
