"""European Bank for Reconstruction and Development (EBRD)

Data is retrieved by downloading a CSV file and parsing it for both partial
project details and links to project webpages. The webpages are then scraped
using both rules and LLM prompting to retrieve information on loan amounts,
associated companies, and approval dates.
"""

# Standard library imports
import json
import re
from datetime import datetime
from logging import Logger

# Third-party imports
import numpy as np
import pandas as pd
from bs4 import BeautifulSoup, NavigableString
from django.conf import settings
from google import genai

# Application imports
from common.http import DataRequestClient
from extract.dal import DatabaseClient
from extract.workflows.abstract import (
    ProjectPartialDownloadWorkflow,
    ProjectPartialScrapeWorkflow,
)


class EbrdProjectPartialDownloadWorkflow(ProjectPartialDownloadWorkflow):
    """Downloads and parses a CSV file containing project URLs and data."""

    @property
    def download_url(self) -> str:
        """A link to directly download project results as a CSV file."""
        return "https://www.ebrd.com/content/dam/ebrd_dxp/projectsData.csv"

    def get_projects(self) -> pd.DataFrame:
        """Downloads a CSV file of development projects from EBRD's website.

        Args:
            `None`

        Returns:
            The raw project records.
        """
        # Fetch project data
        r = self._data_request_client.get(
            self.download_url,
            use_random_user_agent=True,
            use_random_delay=True,
        )
        if not r.ok:
            raise RuntimeError(
                "Error fetching data from EBRD. The request failed "
                f'with a "{r.status_code} - {r.reason}" status '
                f'code and the message "{r.text}".'
            )

        # Parse CSV response body into lines of fields
        data = [line.split(",") for line in r.text.split("\n")]

        # Read data into Pandas DataFrame
        return pd.DataFrame(data[1:], columns=data[0] + ["Bumped Link"])

    def clean_projects(self, df: pd.DataFrame) -> tuple[list[str], pd.DataFrame]:
        """Parses project records and URLs to project pages from the raw data.

        Args:
            df: The raw project records.

        Returns:
            A two-item tuple consisting of the new URLs and cleaned records.
        """
        # Add source column
        df["source"] = settings.EBRD_ABBREVIATION.upper()

        # Drop rows without project ids
        df = df[~(df["Project ID"] == "") & ~(df["Project ID"].isna())]

        # Replace NaN values with empty strings
        df = df.replace({np.nan: ""})

        # Parse publication date values to "YYYY-MM-DD" format
        df["Date Disclosed"] = df["Publication date"].apply(
            lambda val: (
                ""
                if not val
                else datetime.strptime(val, "%d %b %Y").strftime("%Y-%m-%d")
            )
        )

        # Adjust project statuses
        df["Project status"] = df["Project status"].apply(
            lambda val: ("Pending Approval" if val == "Passed Final Review" else val)
        )

        # Correct project page URLs, which spread across two columns
        df["URL"] = df.apply(
            lambda row: (
                row["Bumped Link"] if row["Bumped Link"] else row["URL link to project"]
            ),
            axis=1,
        )

        # Drop unnecessary columns
        df = df.drop(columns=["Bumped Link", "URL link to project"])

        # Apply text encoding to selected columns
        cols = ["Title", "Sector", "Country"]
        df[cols] = df[cols].map(
            lambda x: x.encode("raw_unicode_escape").decode("utf-8")
        )

        # Map rows to URLs
        urls = [row["URL"] for _, row in df.iterrows()]

        # Map rows to project records
        col_map = {
            "Country": "countries",
            "Date Disclosed": "date_disclosed",
            "Title": "name",
            "Project ID": "number",
            "Sector": "sectors",
            "source": "source",
            "Project status": "status",
            "URL": "url",
        }
        df = df.rename(columns=col_map)[col_map.values()]

        # Return project URLs and data
        return urls, df


class EbrdProjectPartialScrapeWorkflow(ProjectPartialScrapeWorkflow):
    """Scrapes an EBRD project page for development bank project data."""

    def __init__(
        self,
        data_request_client: DataRequestClient,
        db_client: DatabaseClient,
        logger: Logger,
    ) -> None:
        """Initializes a new instance of an `EbrdProjectPartialScrapeWorkflow`.

        Args:
            data_request_client: A client for making HTTP GET requests while
                adding random delays and rotating user agent headers.

            db_client: A client used to insert and
                update tasks in the database.

            logger: An standard logger instance.

        Returns:
            `None`
        """
        super().__init__(data_request_client, db_client, logger)
        try:
            api_key = settings.GEMINI_API_KEY
        except AttributeError:
            raise RuntimeError(
                "Failed to instantiate EbrdProjectScrapeWorkflow. "
                "Gemini API key not found in project settings."
            ) from None
        self._gemini_client = genai.Client(api_key=api_key)

    def _build_prompt(self, soup: BeautifulSoup) -> str:
        """Builds a prompt for an LLM to extract loan details from a webpage.

        Args:
            soup: The BeautifulSoup object representing the webpage.

        Returns:
            The prompt.
        """
        # Initialize search text components
        components = []

        # Extract text of finance header
        finance_header_str = soup.find(
            string=re.compile(r"EBRD Finance", re.IGNORECASE)
        )
        finance_header = finance_header_str.find_parent()
        components.append(finance_header.text.strip("\r\n\t "))

        # Extract content in finance section
        finance_section = finance_header.find_next_sibling("p")
        components.append(finance_section.text.strip("\r\n\t "))

        # Extract text of client header
        client_header = [
            tag.find_parent("h2")
            for tag in soup.find_all(string=re.compile(r"Client", re.IGNORECASE))
            if tag.parent.name == "h2"
        ][0]
        components.append(client_header.text.strip("\r\n\t "))

        # Extract content in client section
        client_section = client_header.find_next_sibling("p")
        components.append(client_section.text.strip("\r\n\t "))

        # Finalize search text
        search_text = "\n".join(components)

        # Finalize prompt
        return (
            "You are scraping a webpage for details about a loan financed by "
            "a development bank. Read the text below and return a JSON object "
            'containing the fields "loan_amount", "currency", and "client" '
            'receiving the loan.  Please note that "loan_amount" is measured '
            f"in single units of currency:\n\n{search_text}"
        )

    def _prompt_ai(self, prompt: str) -> dict | None:
        """Prompts the Gemma 3 27B model to extract loan details.

        Args:
            prompt: The prompt to submit.

        Returns:
            The loan and company details if they were
                successfully extracted or `None` otherwise.
        """
        # Prompt model
        response = self._gemini_client.models.generate_content(
            model="gemma-3-27b-it", contents=prompt
        )

        # Extract serialized JSON from response text
        pattern = r"```json\s*(\{.*?\})\s*```"
        match = re.search(pattern, response.text, re.DOTALL)

        # If JSON string not found, raise error
        try:
            payload = json.loads(match.group(1))
        except Exception:
            raise ValueError("Could not parse JSON from response text.") from None

        # Otherwise, validate JSON schema and return
        if "loan_amount" not in payload:
            raise ValueError("Loan amount field not found in JSON.")
        if "currency" not in payload:
            raise ValueError("Currency field not found in JSON.")
        if "client" not in payload:
            raise ValueError("Client field not found in JSON.")

        return payload

    def scrape_project_page(self, url: str) -> list[dict]:
        """Scrapes an EBRD project page for data.

        Leverages the Gemini API service to extract loan
        and company details from the raw text of webpage
        when rule-based webscraping fails. If the service
        fails to produce a JSON response, the fields are
        left empty.

        Args:
            url: The URL for a project.

        Returns:
            `None`
        """
        # Retrieve HTML from webpage
        response = self._data_request_client.get(
            url, use_random_user_agent=True, use_random_delay=True
        )

        # Parse HTML into node tree
        soup = BeautifulSoup(response.text, "html.parser")

        # Parse date field to retrieve year, month, and day
        try:
            date_header_str = soup.find(string="Approval Date")
            date_header = date_header_str.find_parent()
            date_section = date_header.find_next_sibling("p")
            parsed_date = datetime.strptime(date_section.text, "%d %b %Y")
            approved_utc = parsed_date.strftime("%Y-%m-%d")
        except AttributeError:
            approved_utc = ""

        # Parse client information field for company names
        try:
            client_header = [
                tag.find_parent("h2")
                for tag in soup.find_all(string=re.compile(r"Client", re.IGNORECASE))
                if tag.parent.name == "h2"
            ][0]
            client_content = [
                sib
                for sib in client_header.next_siblings
                if not (isinstance(sib, NavigableString) and sib.strip("\r\n\t ") == "")
            ][0]
            companies = (
                client_content.strip("\r\n\t ")
                if isinstance(client_content, NavigableString)
                else ""
            )
        except (AttributeError, IndexError):
            companies = ""

        # Parse loan amount field to retrieve value and currency type
        try:
            finance_header_str = soup.find(
                string=re.compile(r"EBRD Finance", re.IGNORECASE)
            )
            finance_header = finance_header_str.find_parent()
            finance_section = finance_header.find_next_sibling("p")
            loan_amount = finance_section.text.strip("\r\n\t ")
            loan_amount_currency, loan_amount_value = loan_amount.split()
            loan_amount_value = int(float(loan_amount_value.replace(",", "")))
        except (AttributeError, ValueError):
            loan_amount_value = None
            loan_amount_currency = ""

        # Fallback to AI if rule-based webscraping fails
        if not companies or not loan_amount_value or not loan_amount_currency:
            try:
                # Compose prompt from HTML
                prompt = self._build_prompt(soup)

                # Submit prompt
                response = self._prompt_ai(prompt)

                # Parse LLM response
                if response:
                    # Finalize company field
                    companies = (
                        response["client"] if not companies else companies
                    ) or ""

                    # Finalize currency field
                    loan_amount_currency = (
                        response["currency"]
                        if not loan_amount_currency
                        else loan_amount_currency
                    ) or ""

                    # Finalize loan amount field
                    raw_loan_amount_value = (
                        response["loan_amount"]
                        if loan_amount_value is None
                        else loan_amount_value
                    )
                    loan_amount_value = (
                        int(float(raw_loan_amount_value.replace(",", "")))
                        if isinstance(raw_loan_amount_value, str)
                        else raw_loan_amount_value
                    )
            except ValueError:
                loan_amount_value = None
                loan_amount_currency = ""
            except Exception as e:
                self._logger.warning(
                    f"Failed to extract project data for EBRD using Gemini API service: {e}"
                )

        # Compose final project record schema
        return [
            {
                "affiliates": companies,
                "date_approved": approved_utc,
                "source": settings.EBRD_ABBREVIATION.upper(),
                "total_amount": loan_amount_value,
                "total_amount_currency": loan_amount_currency,
                "total_amount_usd": (
                    loan_amount_value if loan_amount_currency == "USD" else None
                ),
                "url": url,
            }
        ]
