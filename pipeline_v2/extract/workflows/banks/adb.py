"""Data extraction workflows for the Asian Development Bank (ADB).
Data is retrieved by scraping all individual project page URLs from
search result pages and then scraping details from each project page.
"""

# Standard library imports
import re
from datetime import datetime
from typing import Dict, List

# Third-party imports
from bs4 import BeautifulSoup
from django.conf import settings

# Application imports
from extract.workflows.abstract import (
    ProjectScrapeWorkflow,
    ResultsScrapeWorkflow,
    SeedUrlsWorkflow,
)


class AdbSeedUrlsWorkflow(SeedUrlsWorkflow):
    """Retrieves the first set of ADB URLs to scrape."""

    @property
    def first_page_num(self) -> int:
        """The number of the first search results page."""
        return 0

    @property
    def next_workflow(self) -> str:
        """The name of the workflow to execute after this
        workflow has finished.
        """
        return settings.RESULTS_PAGE_WORKFLOW

    @property
    def search_results_base_url(self) -> str:
        """The base URL for a development bank project search
        results page on ADB's website. Should be formatted
        with a page number variable, "page_num".
        """
        return "https://www.adb.org/projects?page={page_num}"

    def generate_seed_urls(self) -> List[str]:
        """Generates the first set of URLs to scrape.

        Args:
            None

        Returns:
            The unique list of search result pages.
        """
        try:
            last_page_num = self.find_last_page()
            result_pages = [
                self.search_results_base_url.format(page_num=n)
                for n in range(self.first_page_num, last_page_num + 1)
            ]
            return result_pages
        except Exception as e:
            raise Exception(f"Failed to generate ADB search result pages to crawl. {e}")

    def find_last_page(self) -> int:
        """Retrieves the number of the last page of
        development bank projects on the website.

        Args:
            None

        Returns:
            The page number.
        """
        try:
            params = {"page_num": self.first_page_num}
            first_results_page = self.search_results_base_url.format(**params)
            html = self._data_request_client.get(first_results_page).text
            soup = BeautifulSoup(html, "html.parser")
            last_page_btn = soup.find("li", {"class": "pager__item--last"})
            last_page_num = int(last_page_btn.find("a")["href"].split("=")[-1])
            return last_page_num
        except Exception as e:
            raise Exception(
                f"Error retrieving last page number at '{first_results_page}'. {e}"
            )


class AdbResultsScrapeWorkflow(ResultsScrapeWorkflow):
    """Scrapes an ADB search results page for development bank project URLs."""

    @property
    def project_page_base_url(self) -> str:
        """The base URL for individual ADB project pages."""
        return "https://www.adb.org"

    def scrape_results_page(self, results_page_url: str) -> List[str]:
        """Scrapes all development project page URLs from a given
        search results page on ADB's website. NOTE: Delays must
        be placed in between requests to avoid throttling.

        Args:
            results_page_url: The URL to a search results page
                containing lists of development projects.

        Returns:
            The list of scraped project page URLs.
        """
        try:
            response = self._data_request_client.get(
                url=results_page_url,
                use_random_user_agent=True,
                use_random_delay=True,
                min_random_delay=1,
                max_random_delay=3,
            )
            soup = BeautifulSoup(response.text, features="html.parser")
            projects_table = soup.find("div", {"class": "list"})

            project_page_urls = []
            for project in projects_table.find_all("div", {"class": "item"}):
                try:
                    link = project.find("a")
                    project_page_urls.append(self.project_page_base_url + link["href"])
                except TypeError:
                    continue

            return project_page_urls

        except Exception as e:
            raise RuntimeError(
                f"Error scraping project page URLs from '{results_page_url}'. {e}"
            )


class AdbProjectScrapeWorkflow(ProjectScrapeWorkflow):
    """Scrapes an ADB project page for development bank project data."""

    def scrape_project_page(self, url: str) -> List[Dict]:
        """Scrapes an AFDB project page for data. NOTE: Delays
        must be placed in between requests to avoid throttling.

        Args:
            url: The URL for a project.

        Returns:
            The list of project records.
        """
        # Request page
        r = self._data_request_client.get(
            url=url,
            use_random_user_agent=True,
            use_random_delay=True,
            min_random_delay=1,
            max_random_delay=4,
        )

        # Parse HTML
        soup = BeautifulSoup(r.text, features="html.parser")

        # Find first project table holding project background details
        table = soup.find("article")

        # Extract project name, number, and status
        def get_field(detail_name):
            try:
                element = table.find(string=detail_name)
                parent = element.find_parent()
                sibling_cell = parent.find_next_siblings("dd")[0]
                raw_text = sibling_cell.text
                field = raw_text.strip(" \n")
                return None if field == "" else field
            except AttributeError:
                return None

        name = get_field("Project Name")
        number = get_field("Project Number")
        status = get_field("Project Status")

        # Extract and format countries
        country_label = table.find(string="Country / Economy")
        if not country_label:
            country_label = table.find(string="Country")

        parent = country_label.find_parent()
        sibling_cell = parent.find_next_siblings("dd")[0]
        countries = "|".join(li.text for li in sibling_cell.find_all("li"))

        # Define local function to calculate loan amount multiplier
        def get_multiplier(text) -> float:
            """Returns the multiplier for loan amounts.

            Args:
                text: The loan amount.

            Returns:
                The multiplier.
            """
            if "BILLION" in text:
                return 10**9
            elif "MILLION" in text:
                return 10**6
            else:
                return 1

        # Extract ADB funding amount
        finance_tables = soup.find_all("table", {"class": "fund-table"})
        if not finance_tables:
            loan_amount = None
        else:
            loan_amount = 0
            for t in finance_tables:
                # Find table body
                tbody = t.find("tbody")

                # Find table rows
                rows = tbody.find_all("tr", class_=lambda c: c != "subhead")

                # Parse loan amount and add to total
                for r in rows:
                    loan_cell = r.find_all("td")[1]
                    multiplier = get_multiplier(loan_cell.text.upper())
                    loan_str = re.search(r"([\d,\.]+)", loan_cell.text).groups(0)[0]
                    fund_amount = float(loan_str.replace(",", "")) * multiplier
                    loan_amount += int(fund_amount)
            loan_amount = int(loan_amount)

        # Extract sectors
        sector_header_str = table.find(string="Sector / Subsector")
        sector_row = sector_header_str.find_parent()
        sector_names = sector_row.find_next_sibling("dd")
        sector_strongs = sector_names.find_all("strong", {"class": "sector"})
        sectors = (
            None if not sector_strongs else "|".join(s.text for s in sector_strongs)
        )

        # Extract companies
        try:
            agency_text = soup.find(string="Implementing Agency")
            if not agency_text:
                agency_text = soup.find(string="Executing Agencies")
            parent = agency_text.find_parent()
            agency_cell = parent.find_next_siblings("dd")[0]
            company_spans = agency_cell.find_all("span", {"class": "address-company"})
            companies = "|".join(c.text.strip(" \n") for c in company_spans if c.text)
        except Exception:
            companies = None

        # Define local function to parse date string
        def parse_date(date_str):
            try:
                parsed_date = datetime.strptime(date_str, "%d %b %Y")
                return parsed_date.strftime("%Y-%m-%d")
            except Exception:
                return None

        # Extract project approval date
        try:
            approval_text = soup.find(string="Approval")
            parent = approval_text.find_parent()
            approval_cell = parent.find_next_siblings("dd")[0]
            approved_utc = parse_date(approval_cell.text)
        except Exception:
            approved_utc = None

        # Extract additional project dates
        try:
            milestone_text = soup.find("caption", string="Milestones")
            parent = milestone_text.find_parent()
            labels = [
                th.text.strip(" \n")
                for th in parent.find_all("th")
                if th.text.strip(" \n") != "Closing"
            ]
            values = [td.text.strip(" \n") for td in parent.find_all("td")]
            milestone_dict = dict(zip(labels, values))
            signed_utc = parse_date(milestone_dict.get("Signing Date"))
            effective_utc = parse_date(milestone_dict.get("Effectivity Date"))
            closed_original_utc = parse_date(milestone_dict.get("Original"))
            closed_revised_utc = parse_date(milestone_dict.get("Revised"))
            closed_actual_utc = parse_date(milestone_dict.get("Actual"))
        except Exception:
            signed_utc = None
            effective_utc = None
            closed_original_utc = None
            closed_revised_utc = None
            closed_actual_utc = None

        # Compose final project record schema
        return [
            {
                "bank": settings.ADB_ABBREVIATION.upper(),
                "number": number,
                "name": name,
                "status": status,
                "approved_utc": approved_utc,
                "signed_utc": signed_utc,
                "effective_utc": effective_utc,
                "closed_original_utc": closed_original_utc,
                "closed_revised_utc": closed_revised_utc,
                "closed_actual_utc": closed_actual_utc,
                "loan_amount": loan_amount,
                "loan_amount_currency": "USD" if loan_amount else None,
                "loan_amount_in_usd": loan_amount,
                "sectors": sectors,
                "countries": countries,
                "companies": companies,
                "url": url,
            }
        ]
