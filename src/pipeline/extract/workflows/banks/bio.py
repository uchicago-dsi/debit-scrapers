"""Belgian Investment Company for Developing Countries (BIO)

Data is retrieved by scraping project search result webpages for project
data and links to project overview pages. The overview pages are then
scraped themselves for information on project sectors and companies.
"""

# Standard library imports
import re
from datetime import datetime

# Third-party imports
from bs4 import BeautifulSoup
from django.conf import settings

# Application imports
from extract.workflows.abstract import (
    ProjectPartialScrapeWorkflow,
    ResultsMultiScrapeWorkflow,
    SeedUrlsWorkflow,
)


class BioSeedUrlsWorkflow(SeedUrlsWorkflow):
    """Retrieves the first set of BIO URLs to scrape."""

    @property
    def next_workflow(self) -> str:
        """The next workflow to execute."""
        return settings.RESULTS_PAGE_MULTISCRAPE_WORKFLOW

    @property
    def first_page_num(self) -> int:
        """The starting number for project search result pages."""
        return 1

    @property
    def num_projects_per_page(self) -> int:
        """The number of projects displayed on each search results page."""
        return 9

    @property
    def search_results_base_url(self) -> str:
        """The base URL for a BIO project search results webpage."""
        return "https://www.bio-invest.be/en/investments/p{}?search="

    def _find_last_page(self) -> int:
        """Retrieves the number of the last search results page.

        Args:
            `None`

        Returns:
            The page number.
        """
        try:
            first_results_page = self.search_results_base_url.format(
                self.first_page_num
            )
            html = self._data_request_client.get(first_results_page).text
            soup = BeautifulSoup(html, "html.parser")

            results_div = soup.find("div", {"class": "js-filter-results"})
            num_results_text = results_div.find("small").text
            num_results = int(num_results_text.split(" ")[0])

            last_page_num = (num_results // self.num_projects_per_page) + (
                1 if num_results % self.num_projects_per_page > 0 else 0
            )

            return last_page_num

        except Exception as e:
            raise RuntimeError(
                f"Error retrieving last page number at '{first_results_page}'. {e}"
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
            result_pages = [
                self.search_results_base_url.format(n)
                for n in range(self.first_page_num, last_page_num + 1)
            ]
            return result_pages
        except Exception as e:
            raise RuntimeError(
                f"Failed to generate BIO search result pages to crawl. {e}"
            ) from None


class BioResultsMultiScrapeWorkflow(ResultsMultiScrapeWorkflow):
    """Scrapes a BIO search results page for both project data and URLs."""

    def scrape_results_page(self, url: str) -> tuple[list[str], list[dict]]:
        """Scrapes a search result page for project data and webpage URLs.

        Args:
            url: The URL to a search results page containing
                lists of development projects.

        Returns:
            A tuple consisting of the list of scraped project page
                URLs and list of project records.
        """
        # Retrieve search results page
        response = self._data_request_client.get(url)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, "html.parser")

        # Scrape page for both project data and project page URLs
        project_page_urls = []
        projects = []
        for div in soup.find_all("div", {"class": "card"}):
            # Extract project name and URL
            card_header = div.find("h3", {"class": "card__title"})
            name = card_header.text.strip()
            url = card_header.find("a")["href"]

            # Extract project date
            try:
                raw_date = (
                    div.find(class_="icon--calendar")
                    .find_parent()
                    .text.strip()
                )
                effective_utc = datetime.strptime(
                    raw_date, "%d/%m/%Y"
                ).strftime("%Y-%m-%d")
            except AttributeError:
                effective_utc = ""

            # Extract project countries
            try:
                country_div = div.find(class_="icon--location").find_parent()
                country_arr = [c.strip() for c in country_div.text.split(",")]
                countries = "|".join(country_arr)
            except AttributeError:
                countries = ""

            # Extract loan amount (EUR)
            try:
                loan_amount_str = (
                    div.find(class_="icon--euro").find_parent().text.strip()
                )
                loan_amount_match = re.search(
                    r"([\d,\.]+)", loan_amount_str
                ).groups(0)[0]
                loan_amount_value = float(loan_amount_match.replace(",", ""))
                loan_amount_currency = "EUR"
            except AttributeError:
                loan_amount_value = None
                loan_amount_currency = ""

            # Append results
            project_page_urls.append(url)
            projects.append(
                {
                    "countries": countries,
                    "date_effective": effective_utc,
                    "name": name,
                    "source": settings.BIO_ABBREVIATION.upper(),
                    "total_amount": loan_amount_value,
                    "total_amount_currency": loan_amount_currency,
                    "url": url,
                }
            )

        return project_page_urls, projects


class BioProjectPartialScrapeWorkflow(ProjectPartialScrapeWorkflow):
    """Scrapes a BIO project webpage for development bank project data."""

    def scrape_project_page(self, url: str) -> list[dict]:
        """Scrapes a project webpage for data.

        Args:
            url: The URL for a project.

        Returns:
            The project records.
        """
        # Retrieve HTML
        response = self._data_request_client.get(url)
        soup = BeautifulSoup(response.text, "html.parser")

        # Retrieve project companies
        try:
            company_div = soup.find(string="Organisation").parent
            companies = company_div.find_next_sibling("p").text
        except AttributeError:
            companies = ""

        # Retrieve investment field type
        try:
            inv_field_div = soup.find(string="Investment field").parent
            inv_field = inv_field_div.find_next_sibling("p").text
        except AttributeError:
            inv_field = ""

        # Retrieve investment activity type
        try:
            inv_activity_div = soup.find(string="Activity").parent
            inv_activity = (
                inv_activity_div.find_next_sibling("div").find("p").text
            )
        except AttributeError:
            inv_activity = ""

        # Derive project sector type
        if inv_field.lower() in (
            "investment companies & funds",
            "financial institutions",
        ):
            sectors = "Finance"
        else:
            sectors = f"{inv_field}: {inv_activity}"

        # Compose partial project record schema
        return [
            {
                "affiliates": companies,
                "sectors": sectors,
                "source": settings.BIO_ABBREVIATION.upper(),
                "url": url,
            }
        ]
