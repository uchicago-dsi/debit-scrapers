"""Dutch Entrepreneurial Development Bank (FMO)

Data is retrieved by scraping all individual project page URLs from
search result pages and then scraping details from each project page.
"""

# Standard library imports
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


class FmoSeedUrlsWorkflow(SeedUrlsWorkflow):
    """Retrieves the first set of FMO URLs to scrape."""

    @property
    def first_page_num(self) -> int:
        """The number of the first search results page."""
        return 1

    @property
    def next_workflow(self) -> str:
        """The next workflow to execute."""
        return settings.RESULTS_PAGE_WORKFLOW

    @property
    def search_results_base_url(self) -> str:
        """The base URL for a FMO project search results webpage."""
        return "https://www.fmo.nl/worldmap?page={}"

    def _find_last_page(self) -> int:
        """Retrieves the number of the last search results page.

        Args:
            `None`

        Returns:
            The page number.
        """
        try:
            first_page_url = self.search_results_base_url.format(self.first_page_num)
            html = self._data_request_client.get(
                first_page_url,
                use_random_user_agent=True,
                use_random_delay=True,
            ).text
            soup = BeautifulSoup(html, "html.parser")

            pager = soup.find("div", {"class": "pbuic-pager-container"})
            last_page_num = int(pager.find_all("a")[-2]["href"].split("=")[-1])
            return last_page_num
        except Exception as e:
            raise RuntimeError(f"Error retrieving last page number. {e}") from None

    def generate_seed_urls(self) -> list[str]:
        """Generates the first set of URLs to scrape.

        Args:
            `None`

        Returns:
            The unique list of search result pages.
        """
        try:
            last_page_num = self._find_last_page()
            results_page_urls = [
                self.search_results_base_url.format(n)
                for n in range(1, last_page_num + 1)
            ]
            return results_page_urls
        except Exception as e:
            raise RuntimeError(
                f"Failed to generate search result pages to crawl. {e}"
            ) from None


class FmoResultsScrapeWorkflow(ResultsScrapeWorkflow):
    """Scrapes an FMO search results page for development bank project URLs."""

    def scrape_results_page(self, url: str) -> list[str]:
        """Scrapes a search results page for project webpage URLs.

        Args:
            url: The URL to a search results page
                containing lists of development projects.

        Returns:
            The list of scraped project page URLs.
        """
        try:
            source = self._data_request_client.get(
                url, use_random_user_agent=True, use_random_delay=True
            ).text
            soup = BeautifulSoup(source, "html.parser")
            urls = [
                proj["href"]
                for proj in soup.find_all("a", {"class": "ProjectList__projectLink"})
            ]
            return urls
        except Exception as e:
            raise RuntimeError(
                f"Error scraping '{url}' for project URLs. {e}"
            ) from None


class FmoProjectScrapeWorkflow(ProjectScrapeWorkflow):
    """Scrapes an FMO project page for development bank project data."""

    def scrape_project_page(self, url: str) -> list[dict]:
        """Scrapes a FMO project page for data.

        Args:
            url: The URL for a project.

        Returns:
            The project records.
        """
        try:
            # Extract project number from URL
            number = url.split("/")[-1] if url else ""

            # Fetch webpage HTML
            r = self._data_request_client.get(
                url, use_random_user_agent=True, use_random_delay=True
            )

            # Raise error if request failed
            if not r.ok:
                raise RuntimeError(
                    "Error fetching data from FMO. "
                    f"The request failed with a "
                    f'"{r.status_code} - {r.reason}" status '
                    f'code and the message "{r.text}".'
                )

            # Parse HTML into node tree
            soup = BeautifulSoup(r.content, "html.parser")

            # Extract project name
            name = soup.find("h2", class_="ProjectDetail__title").text.strip()

            # Extract project status
            try:
                status_span = soup.find(
                    "span", class_="ProjectDetail__titleProposedInvestment"
                )
                status = status_span.text.split(":")[-1].strip()
            except Exception:
                status = ""

            # Create project detail lookup
            detail_div = soup.find("div", class_="ProjectDetail__aside--right")
            detail_table = detail_div.find("dl")
            detail_headers = [dt.text for dt in detail_table.find_all("dt")]
            detail_values = [dd.text for dd in detail_table.find_all("dd")]
            detail_lookup = dict(zip(detail_headers, detail_values))

            # Extract project sectors
            try:
                sectors = "|".join(detail_lookup["Sector"].split(", "))
            except Exception:
                sectors = ""

            # Extract project countries
            try:
                countries = detail_lookup["Country"]
                name_parts = countries.split(",")
                num_formal_name_parts = 2
                uses_formal_name = len(name_parts) == num_formal_name_parts
                if uses_formal_name:
                    countries = f"{name_parts[1].strip()} {name_parts[0]}"
            except Exception:
                countries = ""

            # Define local function to get multiplier
            def get_multiplier(amount_str: str) -> float | None:
                """Returns the multiplier for loan amounts.

                Args:
                    amount_str: The loan amount.

                Returns:
                    The multiplier.
                """
                try:
                    if "MLN" in amount_str.upper():
                        return 10**6
                    elif "BLN" in amount_str.upper():
                        return 10**9
                    else:
                        return 1
                except AttributeError:
                    return None

            # Parse financing field for loan amount and currency type
            try:
                financing = detail_lookup["Total FMO financing"]
                if financing and financing != "n.a.":
                    total_amount_currency, total_amount, _ = financing.split(" ")
                    total_amount = float(total_amount) * get_multiplier(financing)
                else:
                    raise Exception()
            except Exception:
                total_amount = None
                total_amount_currency = ""

            # Define function to format date
            def get_date(field_name_str: str) -> str:
                try:
                    raw_date = detail_lookup[field_name_str]
                    parsed_date = datetime.strptime(raw_date, "%m/%d/%Y")
                    return parsed_date.strftime("%Y-%m-%d")
                except Exception:
                    return ""

            # Parse date to retrieve year, month, and day
            disclosed_utc = get_date("Publication date")
            effective_utc = get_date("Effective date")
            closed_original_utc = get_date("End date")

            # Compose final project record schema
            return [
                {
                    "countries": countries,
                    "date_disclosed": disclosed_utc,
                    "date_effective": effective_utc,
                    "date_planned_close": closed_original_utc,
                    "name": name,
                    "number": number,
                    "sectors": sectors,
                    "source": settings.FMO_ABBREVIATION.upper(),
                    "status": status,
                    "total_amount": total_amount,
                    "total_amount_currency": total_amount_currency,
                    "url": url,
                }
            ]

        except Exception as e:
            raise RuntimeError(f"Error scraping project page '{url}'. {e}") from None
