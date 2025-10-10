"""Proparco (PRO)

Data is retrieved by scraping project search result pages for
project webpage URLs and then iteratively scraping those webpages
for project details.
"""

# Standard library imports
import re
from datetime import datetime

# Third-party imports
from bs4 import BeautifulSoup
from django.conf import settings

# Application imports
from extract.workflows.abstract import (
    ProjectScrapeWorkflow,
    ResultsScrapeWorkflow,
    SeedUrlsWorkflow,
)


class ProSeedUrlsWorkflow(SeedUrlsWorkflow):
    """Retrieves the first set of Proparco URLs to scrape."""

    @property
    def next_workflow(self) -> str:
        """The next workflow to execute."""
        return settings.RESULTS_PAGE_WORKFLOW

    @property
    def search_results_base_url(self) -> str:
        """The base URL for a project search results webpage."""
        return "https://www.proparco.fr/en/projects/list"

    def generate_seed_urls(self) -> list[str]:
        """Generates the first set of URLs to scrape.

        Args:
            `None`

        Returns:
            The project page URLs.
        """
        try:
            # Request project page
            r = self._data_request_client.get(
                self.search_results_base_url,
                use_random_user_agent=True,
                use_random_delay=True,
            )

            # Confirm that request was successful
            if not r.ok:
                raise RuntimeError(
                    "Error retrieving first search results page "
                    "from Proparco.The request failed with a "
                    f'"{r.status_code} - {r.reason}" status '
                    f'code and the message "{r.text}".'
                )

            # Parse webpage HTML into node tree
            soup = BeautifulSoup(r.text, "lxml")

            # Scrape last page of search results
            last_anchor = soup.find("a", class_="fr-pagination__link--last")
            last_page = int(last_anchor["href"].split("=")[1])

            # Build links to search result pages
            return [
                f"{self.search_results_base_url}?page={page_num}"
                for page_num in range(last_page + 1)
            ]
        except Exception as e:
            raise RuntimeError(
                f"Error retrieving list of project page URLs. {e}"
            ) from None


class ProResultsScrapeWorkflow(ResultsScrapeWorkflow):
    """Scrapes a Proparco search results page for links to project webpages."""

    @property
    def site_base_url(self) -> str:
        """The base URL for Proparco's website."""
        return "https://www.proparco.fr"

    @property
    def next_workflow(self) -> str:
        """The next workflow to execute."""
        return settings.PROJECT_PAGE_WORKFLOW

    def scrape_results_page(self, url: str) -> list[str]:
        """Scrapes a search results page for project webpage URLs.

        Args:
            url: The URL for the results page.

        Returns:
            The project page URLs.
        """
        # Request project page
        r = self._data_request_client.get(
            url, use_random_user_agent=True, use_random_delay=True
        )

        # Confirm that request was successful
        if not r.ok:
            raise RuntimeError(
                "Error fetching search results page "
                f"from Proparco. The request failed "
                f'with a "{r.status_code} - {r.reason}" '
                f'status code and the message "{r.text}".'
            )

        # Parse webpage HTML into node tree
        soup = BeautifulSoup(r.text, "lxml")

        # Scrape project page URLs
        anchors = soup.find_all("a", class_="fr-card__link")

        # Scrape project page URLs
        return [f"{self.site_base_url}{a['href']}" for a in anchors]


class ProProjectScrapeWorkflow(ProjectScrapeWorkflow):
    """Scrapes a Proparco project page for development bank project data."""

    def scrape_project_page(self, url: str) -> list[dict]:
        """Scrapes a Proparco project page for data.

        Args:
            url: The URL for a project.

        Returns:
            The project record(s).
        """
        # Retrieve HTML
        r = self._data_request_client.get(
            url, use_random_user_agent=True, use_random_delay=True
        )
        if not r.ok:
            raise RuntimeError(
                "Failed to fetch project "
                f'page at url "{url}" The request '
                f'returned a "{r.status_code}-'
                f'{r.reason}" response with the '
                f'message "{r.text}".'
            )

        # Load HTML
        soup = BeautifulSoup(r.text, "html.parser")

        # Extract project name
        name = (
            soup.find("h1", class_="print-title-page")
            .text.replace("\n", "")
            .strip()
        )

        # Extract project sectors
        try:
            sectors = "|".join(
                tag.text.strip() for tag in soup.find_all("a", class_="fr-tag")
            )
        except AttributeError:
            sectors = ""

        # Get reference to project info table
        info_table = soup.find("div", class_="info-setup")

        # Define local function to extract table values
        def extract_table_value(header: str) -> str:
            """Extracts a value from a table.

            Args:
                header: The table row header (e.g., "Signature Date).

            Returns:
                The table value.
            """
            try:
                header_div = info_table.find("div", string=header)
                val_div = (
                    header_div.find_parent().find_next_sibling().find("div")
                )
                return val_div.text.strip()
            except AttributeError:
                return ""

        # Extract project signature date from table
        try:
            raw_date = extract_table_value("Signature date")
            parsed_date = datetime.strptime(raw_date, "%B %d %Y")
            signed_utc = parsed_date.strftime("%Y-%m-%d")
        except (AttributeError, TypeError):
            signed_utc = ""

        # Extract project finance types from table
        try:
            raw_finance_types = extract_table_value("Financing tool")
            finance_types = "|".join(raw_finance_types.split(", "))
        except (AttributeError, TypeError):
            finance_types = ""

        # Extract project loan amount from table
        try:
            raw_loan_amount = extract_table_value("Financing amount (Euro)")
            parsed_loan_amount = raw_loan_amount.replace(" ", "").replace(
                ",", "."
            )
            loan_amount = float(parsed_loan_amount)
            loan_amount_currency = "EUR"
        except (AttributeError, ValueError):
            loan_amount = None
            loan_amount_currency = ""

        # Extract project countries from table
        try:
            raw_countries = extract_table_value("Location")
            countries = "|".join(raw_countries.split(", "))
            if not countries:
                countries = ""
        except (AttributeError, TypeError):
            countries = ""

        # Extract project companies from table
        try:
            raw_companies = extract_table_value("Customer")
            companies_sans_details = re.sub(r"\([^)]*\)", "", raw_companies)
            formatted_companies = (
                companies_sans_details.replace("\n", " ")
                .replace("  ", " ")
                .strip()
            )
            companies = "|".join(formatted_companies.split(", "))
        except (AttributeError, TypeError):
            companies = ""

        # Extract project number from table
        number = extract_table_value("Project number")

        # Compose final project record schema
        return [
            {
                "affiliates": companies,
                "countries": countries,
                "date_signed": signed_utc,
                "finance_types": finance_types,
                "name": name,
                "number": number,
                "sectors": sectors,
                "source": settings.PRO_ABBREVIATION.upper(),
                "status": "Signed" if signed_utc else "",
                "total_amount": loan_amount,
                "total_amount_currency": loan_amount_currency,
                "url": url,
            }
        ]
