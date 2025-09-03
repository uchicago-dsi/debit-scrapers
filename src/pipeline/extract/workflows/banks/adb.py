"""Asian Development Bank (ADB)

Data is retrieved by scraping all individual project page URLs from
search result pages and then scraping details from each project page.
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


class AdbSeedUrlsWorkflow(SeedUrlsWorkflow):
    """Retrieves the first set of ADB URLs to scrape."""

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
        """The base URL for a project search results webpage."""
        return "https://www.adb.org/projects?page={page_num}"

    def _find_last_page(self) -> int:
        """Retrieves the number of the last search results page.

        Args:
            `None`

        Returns:
            The page number.
        """
        try:
            params = {"page_num": self.first_page_num}
            first_results_page = self.search_results_base_url.format(**params)
            html = self._data_request_client.get(
                first_results_page,
                use_random_delay=True,
                use_random_user_agent=True,
            ).text
            soup = BeautifulSoup(html, "html.parser")
            last_page_btn = soup.find("li", {"class": "pager__item--last"})
            last_page_num = int(last_page_btn.find("a")["href"].split("=")[-1])
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
                self.search_results_base_url.format(page_num=num)
                for num in range(self.first_page_num, last_page_num + 1)
            ]
            return result_pages
        except Exception as e:
            raise RuntimeError(
                f"Failed to generate ADB search result pages to crawl. {e}"
            ) from None


class AdbResultsScrapeWorkflow(ResultsScrapeWorkflow):
    """Scrapes an ADB search results page for development bank project URLs."""

    @property
    def project_page_base_url(self) -> str:
        """The base URL for individual ADB project pages."""
        return "https://www.adb.org"

    def scrape_results_page(self, url: str) -> list[str]:
        """Scrapes a search results page for project webpage URLs.

        NOTE: Delays must be placed in between requests to avoid throttling.

        Args:
            url: The URL to a search results page
                containing lists of development projects.

        Returns:
            The list of scraped project page URLs.
        """
        # Request page
        r = self._data_request_client.get(
            url=url,
            use_random_user_agent=True,
            use_random_delay=True,
            min_random_delay=1,
            max_random_delay=3,
        )

        # Check response
        if not r.ok:
            raise RuntimeError(
                f"Error fetching search results page "
                f"from ADB. The request failed with a "
                f'"{r.status_code} - {r.reason}" status '
                f'code and the message "{r.text}".'
            )

        # Scrape search results
        try:
            soup = BeautifulSoup(r.text, features="html.parser")
            projects_table = soup.find("div", {"class": "list"})
            urls = []
            for project in projects_table.find_all("div", {"class": "item"}):
                try:
                    link = project.find("a")
                    urls.append(self.project_page_base_url + link["href"])
                except TypeError:
                    continue
        except Exception as e:
            raise RuntimeError(
                f"Error scraping project page URLs from '{url}'. {e}"
            ) from None

        return urls


class AdbProjectScrapeWorkflow(ProjectScrapeWorkflow):
    """Scrapes an ADB project page for development bank project data."""

    def scrape_project_page(self, url: str) -> list[dict]:
        """Extracts project details from an ADB project webpages.

        NOTE: Delays must be placed in between requests to avoid throttling.

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
        def get_field(detail_name: str) -> str:
            try:
                element = table.find(string=detail_name)
                parent = element.find_parent()
                sibling_cell = parent.find_next_siblings("dd")[0]
                raw_text = sibling_cell.text
                field = raw_text.strip(" \n")
                return "" if field == "" else field
            except AttributeError:
                return ""

        name = get_field("Project Name")
        number = get_field("Project Number")
        status = get_field("Project Status")
        finance_types = get_field("Project Type / Modality of Assistance")

        # Extract and format countries
        country_label = table.find(string="Country / Economy")
        if not country_label:
            country_label = table.find(string="Country")

        parent = country_label.find_parent()
        sibling_cell = parent.find_next_siblings("dd")[0]
        countries = "|".join(li.text for li in sibling_cell.find_all("li"))

        # Define local function to calculate loan amount multiplier
        def get_multiplier(text: str) -> float:
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
                    loan_str = re.search(r"([\d,\.]+)", loan_cell.text).groups(
                        0
                    )[0]
                    fund_amount = float(loan_str.replace(",", "")) * multiplier
                    loan_amount += int(fund_amount)
            loan_amount = int(loan_amount)

        # Extract sectors
        sector_header_str = table.find(string="Sector / Subsector")
        sector_row = sector_header_str.find_parent()
        sector_names = sector_row.find_next_sibling("dd")
        sector_strongs = sector_names.find_all("strong", {"class": "sector"})
        sectors = (
            ""
            if not sector_strongs
            else "|".join(s.text for s in sector_strongs)
        )

        # Extract companies
        try:
            agency_text = soup.find(string="Implementing Agency")
            if not agency_text:
                agency_text = soup.find(string="Executing Agencies")
            parent = agency_text.find_parent()
            agency_cell = parent.find_next_siblings("dd")[0]
            company_spans = agency_cell.find_all(
                "span", {"class": "address-company"}
            )
            affiliates = "|".join(
                c.text.strip(" \n") for c in company_spans if c.text
            )
        except Exception:
            affiliates = ""

        # Define local function to parse date string
        def parse_date(date_str: str) -> str:
            try:
                parsed_date = datetime.strptime(date_str, "%d %b %Y")
                return parsed_date.strftime("%Y-%m-%d")
            except Exception:
                return ""

        # Extract project approval date
        try:
            approval_text = soup.find(string="Approval")
            parent = approval_text.find_parent()
            approval_cell = parent.find_next_siblings("dd")[0]
            date_approved = parse_date(approval_cell.text)
        except Exception:
            date_approved = ""

        # Extract project appraisal date
        try:
            appraisal_text = soup.find(string="Concept Clearance")
            parent = appraisal_text.find_parent()
            appraisal_cell = parent.find_next_siblings("dd")[0]
            date_under_appraisal = parse_date(appraisal_cell.text)
        except Exception:
            date_under_appraisal = ""

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
            date_effective = parse_date(milestone_dict.get("Effectivity Date"))
            date_planned_close = parse_date(milestone_dict.get("Original"))
            date_revised_close = parse_date(milestone_dict.get("Revised"))
            date_actual_close = parse_date(milestone_dict.get("Actual"))
            date_signed = parse_date(milestone_dict.get("Signing Date"))
        except Exception:
            date_effective = ""
            date_planned_close = ""
            date_revised_close = ""
            date_actual_close = ""
            date_signed = ""

        # Compose final project record schema
        return [
            {
                "affiliates": affiliates,
                "countries": countries,
                "date_actual_close": date_actual_close,
                "date_approved": date_approved,
                "date_effective": date_effective,
                "date_planned_close": date_planned_close,
                "date_revised_close": date_revised_close,
                "date_signed": date_signed,
                "date_under_appraisal": date_under_appraisal,
                "finance_types": finance_types,
                "name": name,
                "number": number,
                "sectors": sectors,
                "source": settings.ADB_ABBREVIATION.upper(),
                "status": status,
                "total_amount": loan_amount,
                "total_amount_currency": "USD" if loan_amount else None,
                "total_amount_usd": loan_amount,
                "url": url,
            }
        ]
