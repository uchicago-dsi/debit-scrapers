"""Multilateral Investment Guarantee Agency (MIGA)

Data is retrieved by generating a list of all possible search
result pages, scraping individual project URLs from those pages,
and then scraping fields from each project page and mapping them
to an expected output schema.
"""

# Standard library imports
import re
from collections.abc import Callable
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


class MigaSeedUrlsWorkflow(SeedUrlsWorkflow):
    """Retrieves the first set of MIGA URLs to scrape."""

    @property
    def first_page_num(self) -> int:
        """The number of the first search results page."""
        return 0

    @property
    def next_workflow(self) -> str:
        """The next workflow to execute."""
        return settings.RESULTS_PAGE_WORKFLOW

    @property
    def search_results_base_url(self) -> str:
        """The base URL for a MIGA project search results webpage."""
        return "https://www.miga.org/projects?page={}"

    def _find_last_page(self) -> int:
        """Retrieves the number of the last search results page.

        Args:
            `None`

        Returns:
            The page number.
        """
        try:
            # Fetch search results page
            url = self.search_results_base_url.format(self.first_page_num)
            r = self._data_request_client.get(
                url, use_random_user_agent=True, use_random_delay=True
            )

            # Check if request was successful
            if not r.ok:
                raise Exception(
                    f"Error retrieving first search results page "
                    f"from MIGA. The request failed with a "
                    f'"{r.status_code} - {r.reason}" status '
                    f'code and the message "{r.text}".'
                )

            # Parse webpage HTML into node tree
            soup = BeautifulSoup(r.text, "html.parser")

            # Retrieve last page number
            last = soup.find("li", {"class": "pager__item pager__item--last"})
            last_page = int(last.find("a")["href"].split("=")[1])

            return last_page
        except Exception as e:
            self._logger.error(
                "Error retrieving last page number at "
                f"'{self.search_results_base_url}'. {e}"
            )

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
                self.search_results_base_url.format(page_num)
                for page_num in range(last_page_num + 1)
            ]
            return result_pages
        except Exception as e:
            self._logger.error(f"Failed to generate search result pages to crawl. {e}")


class MigaResultsScrapeWorkflow(ResultsScrapeWorkflow):
    """Scrapes a MIGA search results page for development bank project URLs."""

    @property
    def ifc_disclosures_base_url(self) -> str:
        """The base URL for project disclosures from IFC.

        NOTE: Some MIGA search result pages contain links to
        IFC projects, a fellow member of the World Bank Group.
        """
        return "https://disclosures.ifc.org"

    @property
    def miga_projects_base_url(self) -> str:
        """The base URL for MIGA projects."""
        return "https://www.miga.org"

    def scrape_results_page(self, url: str) -> list[str]:
        """Scrapes a search results page for project webpage URLs.

        Args:
            url: The URL to a search results page
                containing lists of development projects.

        Returns:
            The list of scraped project page URLs.
        """
        try:
            # Fetch search results page
            r = self._data_request_client.get(
                url, use_random_user_agent=True, use_random_delay=True
            )

            # Check if request was successful
            if not r.ok:
                raise Exception(
                    f"Error fetching search results page "
                    f"from MIGA. The request failed with a "
                    f'"{r.status_code} - {r.reason}" status '
                    f'code and the message "{r.text}".'
                )

            # Parse webpage HTML into node tree
            soup = BeautifulSoup(r.text, "html.parser")

            # Identify project container
            projects_ctr_div = soup.find("div", {"class": "featured-projects"})
            projects_div = projects_ctr_div.find(
                "div", {"class": "view-content"}, recursive=False
            )

            # Scrape project page URLs belonging to MIGA only
            urls = []
            for project in projects_div.find_all("h5", {"class": "page-title"}):
                href = project.find("a")["href"]
                if href.startswith(self.ifc_disclosures_base_url):
                    continue
                project_url = (
                    href if "http" in href else self.miga_projects_base_url + href
                )
                urls.append(project_url)

            return urls
        except Exception as e:
            self._logger.error(f"Error scraping project page URLs from '{url}'. {e}")


class MigaProjectScrapeWorkflow(ProjectScrapeWorkflow):
    """Scrapes a MIGA project page for development bank project data."""

    def scrape_project_page(self, url: str) -> list[dict]:
        """Scrapes a MIGA project page for data.

        Args:
            url: The URL for a project.

        Returns:
            The project record(s).
        """
        # Fetch project page
        r = self._data_request_client.get(
            url, use_random_user_agent=True, use_random_delay=True
        )

        # Check if request was successful
        if not r.ok:
            raise Exception(
                f"Error fetching project page "
                f"from MIGA. The request failed with a "
                f'"{r.status_code} - {r.reason}" status '
                f'code and the message "{r.text}".'
            )

        try:
            # Parse webpage HTML into node tree
            soup = BeautifulSoup(r.text, "html.parser")

            # Define local helper function to identify page elements
            def safe_nav(func: Callable) -> str:
                try:
                    html = func(soup)
                    return html.text.strip()
                except AttributeError:
                    return ""

            # Extract project name
            name = safe_nav(lambda s: s.find("h1"))

            # Extract and format project number
            number = safe_nav(
                lambda s: s.find("div", class_="field--name-field-project-id").find(
                    "div", class_="field--item"
                )
            )
            number = number.replace(",", "").replace(" ", ",")

            # Extract project status
            status = safe_nav(
                lambda s: s.find("div", {"class": "field--name-field-project-status"})
            )

            # Extract and format dislosure date
            raw_disclosed_utc = safe_nav(
                lambda s: s.find(
                    "div", class_="field--name-field-date-spg-closed"
                ).find("div", class_="field--item")
            )
            if raw_disclosed_utc:
                parsed_disclosed_utc = datetime.strptime(raw_disclosed_utc, "%B %d, %Y")
                disclosed_utc = parsed_disclosed_utc.strftime("%Y-%m-%d")
            else:
                disclosed_utc = ""

            # Extract and format board approval date
            raw_approved_utc = safe_nav(
                lambda s: s.find("div", class_="field--name-field-board-date").find(
                    "div", class_="field--item"
                )
            )
            if raw_approved_utc:
                parsed_approved_utc = datetime.strptime(raw_approved_utc, "%B %d, %Y")
                approved_utc = parsed_approved_utc.strftime("%Y-%m-%d")
            else:
                approved_utc = ""

            # Extract fiscal year
            fiscal_year = safe_nav(
                lambda s: s.find("div", class_="field--name-field-fiscal-year").find(
                    "div", class_="field--item"
                )
            )

            # Extract and format project countries
            raw_countries = safe_nav(
                lambda s: s.find("div", class_="field--name-field-host-country")
            )
            raw_countries = re.sub("[\r\n\t]", " ", raw_countries)
            raw_countries = raw_countries.split("and")
            formatted_countries = []
            num_formal_name_parts = 2
            for country in raw_countries:
                name_parts = country.split(",")
                uses_formal_country_name = len(name_parts) == num_formal_name_parts
                if uses_formal_country_name:
                    formatted_countries.append(
                        f"{name_parts[1].strip()} {name_parts[0].strip()}"
                    )
                else:
                    formatted_countries.append(name_parts[0].strip())

            countries = ",".join(formatted_countries)

            # Extract project sector
            sectors = safe_nav(
                lambda s: s.find("div", class_="field--name-field-sector").find(
                    "div", class_="field--item"
                )
            )

            # Define function to determine multiplier
            def get_multiplier(amount_str: str) -> float:
                """Returns a multiplier for loan amounts.

                Args:
                    amount_str: The loan amount.

                Returns:
                    The multiplier.
                """
                if "MILLION" in amount_str.upper():
                    return 10**6
                elif "BILLION" in amount_str.upper():
                    return 10**9
                else:
                    return 1

            # Extract and format guarantee amount
            raw_amount = safe_nav(
                lambda s: s.find("div", class_="field--name-field-gross-exposure-up-to")
            )
            if raw_amount:
                multiplier = get_multiplier(raw_amount)
                leading_decimal = re.search(r"(\d+\.*\d*)", raw_amount).group(1)
                amount = float(leading_decimal) * multiplier
            else:
                amount = None

            # Determine currency
            if not raw_amount:
                currency = ""
            elif raw_amount.startswith("$EUR") or raw_amount.startswith("€"):
                currency = "EUR"
            elif raw_amount.startswith("$"):
                currency = "USD"
            else:
                currency = ""

            # Extract affiliates
            try:
                guarantee_div = soup.find(
                    "div", class_="field--name-field-guarantee-holder-term"
                )
                affiliates = "|".join(
                    div.text
                    for div in guarantee_div.find_all("div", class_="field--item")
                )
            except AttributeError:
                affiliates = ""

            return [
                {
                    "affiliates": affiliates,
                    "countries": countries,
                    "date_approved": approved_utc,
                    "date_disclosed": disclosed_utc,
                    "date_effective": fiscal_year,
                    "finance_types": "Guarantee",
                    "name": name,
                    "number": number,
                    "sectors": sectors,
                    "source": settings.MIGA_ABBREVIATION.upper(),
                    "status": status,
                    "total_amount": amount,
                    "total_amount_currency": currency,
                    "total_amount_usd": amount if currency == "USD" else None,
                    "url": url,
                }
            ]

        except Exception as e:
            raise RuntimeError(
                f"Failed to scrape MIGA project page {url}. {e}"
            ) from None
