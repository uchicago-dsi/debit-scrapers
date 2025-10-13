"""Asian Infrastructure Investment Bank (AIIB)

Data is retrieved by requesting a JavaScript file containing
a list of all project page URLs and then requesting and scraping
details from each project page.
"""

# Standard library imports
import json
import re
from datetime import datetime

# Third-party imports
from bs4 import BeautifulSoup
from bs4.element import NavigableString
from django.conf import settings

# Application imports
from extract.workflows.abstract import ProjectScrapeWorkflow, SeedUrlsWorkflow


class AiibSeedUrlsWorkflow(SeedUrlsWorkflow):
    """Retrieves the first set of AIB URLs to scrape."""

    @property
    def projects_base_url(self) -> str:
        """The base URL for an individual AIIB project page."""
        return "https://www.aiib.org"

    @property
    def next_workflow(self) -> str:
        """The next workflow to execute."""
        return settings.PROJECT_PAGE_WORKFLOW

    @property
    def partial_projects_url(self) -> str:
        """The URL containing all partial project records as JSON."""
        return "https://www.aiib.org/en/projects/list/.content/all-projects-data.js"

    def generate_seed_urls(self) -> list[str]:
        """Generates the first set of URLs to scrape.

        Args:
            `None`

        Returns:
            The unique list of search result pages.
        """
        try:
            # Request JavaScript file containing list of projects
            response = self._data_request_client.get(
                url=self.partial_projects_url,
                use_random_user_agent=True,
                use_random_delay=True,
            )

            # Extract project data as string from response body
            stripped_doc = re.sub("[\t\n\r]", "", response.text)
            projects_str = re.findall("(\\[.*\\])", stripped_doc)[0]
            projects_formatted_str = "".join(projects_str.rsplit(",", 1))

            # Parse project string into Python dictionary
            data = json.loads(projects_formatted_str)

            # Generate list of project urls
            return [self.projects_base_url + proj["path"] for proj in data]

        except Exception as e:
            raise RuntimeError(
                f"Failed to generate AIIB search pages to scrape. {e}"
            ) from None


class AiibProjectScrapeWorkflow(ProjectScrapeWorkflow):
    """Scrapes an AIIB project page for development bank project data."""

    def scrape_project_page(self, url: str) -> list[dict]:
        """Scrapes an AIIB project page for data.

        Args:
            url: The URL for a project.

        Returns:
            The project records.
        """
        # Retrieve the project page
        r = self._data_request_client.get(
            url, use_random_user_agent=True, use_random_delay=True
        )

        # Confirm that request was successful
        if not r.ok:
            raise RuntimeError(
                "Error fetching project page "
                f"from AIIB. The request failed "
                f'with a "{r.status_code} - {r.reason}" '
                f'status code and the message "{r.text}".'
            ) from None

        # Parse webpage HTML into node tree
        soup = BeautifulSoup(r.text, "html.parser")

        # Get project summary metadata
        def get_project_summary_field(field_name: str) -> str:
            """Extracts the given data field from the project summary section.

            Args:
                field_name: The field name.

            Returns:
                The extracted text if it exists.
            """
            try:
                div = soup.find(string=field_name).find_next("div")
                return div.text
            except AttributeError:
                return ""

        number = get_project_summary_field("PROJECT NUMBER")
        name = soup.find("h1", {"class": "project-name"}).text
        status = get_project_summary_field("STATUS")
        disclosed = get_project_summary_field("CONCEPT REVIEW")
        appraised = get_project_summary_field("APPRAISAL REVIEW/FINAL REVIEW")
        approved = get_project_summary_field("FINANCING APPROVAL")
        closed = get_project_summary_field("LOAN CLOSING/LAST DISBURSEMENT")
        proposed_funding_amount = get_project_summary_field("PROPOSED FUNDING AMOUNT")
        approved_funding = get_project_summary_field("APPROVED FUNDING")
        sectors = get_project_summary_field("SECTOR")
        countries = get_project_summary_field("MEMBER")

        # Extract project borrower and implementing entity data
        def get_project_contact_field(field_name: str) -> str:
            """Extracts contact information for a borrower or implementer.

            Args:
                field_name: The name of the contact field to scrape.

            Returns:
                The contact information.
            """
            try:
                contact_div = soup.find("h2", string=field_name).find_next_sibling(
                    "div"
                )
                contact_fields = []
                nbsp = "\xa0"
                for p in contact_div.find_all("p"):
                    for c in p.contents:
                        if c and isinstance(c, NavigableString) and c != nbsp:
                            contact_fields.append(c.strip(", "))
                return ", ".join(contact_fields)

            except AttributeError:
                return ""

        borrower = get_project_contact_field("BORROWER")
        implementer = get_project_contact_field("IMPLEMENTING ENTITY")
        if borrower and implementer:
            companies = f"{borrower}|{implementer}"
        elif borrower:
            companies = borrower
        else:
            companies = implementer

        # Parse date fields
        def parse_date(date_str: str) -> str:
            """Maps a date string to a YYYY-MM-DD format.

            Args:
                date_str: The date string to parse.

            Returns:
                The parsed date string, or an empty string if parsing fails.
            """
            try:
                return datetime.strptime(date_str, "%B %d, %Y").strftime("%Y-%m-%d")
            except Exception:
                return ""

        disclosed_utc = parse_date(disclosed)
        appraised_utc = parse_date(appraised)
        approved_utc = parse_date(approved)
        closed_original_utc = parse_date(closed)

        # Fall back to data in URL if missing dates
        if not disclosed_utc and "proposed" in url:
            disclosed_utc = url.split("/")[-3]

        if not approved_utc and "approved" in url:
            approved_utc = url.split("/")[-3]

        # Parse loan amount field to retrieve value and currency type
        if not proposed_funding_amount and not approved_funding:
            loan_amount = None
            loan_amount_currency = ""
        else:
            loan_str = (
                proposed_funding_amount if proposed_funding_amount else approved_funding
            )
            loan_amount_currency = loan_str[:3]
            loan_amount_match = re.search(r"([\d,\.]+)", loan_str).groups(0)[0]
            loan_amount = float(loan_amount_match.replace(",", "")) * 10**6

        # Compose final project record schema
        return [
            {
                "affiliates": companies,
                "countries": countries,
                "date_actual_close": closed_original_utc,
                "date_approved": approved_utc,
                "date_disclosed": disclosed_utc,
                "date_under_appraisal": appraised_utc,
                "name": name,
                "number": number,
                "sectors": sectors,
                "source": settings.AIIB_ABBREVIATION.upper(),
                "status": status,
                "total_amount": loan_amount,
                "total_amount_currency": loan_amount_currency,
                "total_amount_usd": (
                    loan_amount if loan_amount_currency == "USD" else None
                ),
                "url": url,
            }
        ]
