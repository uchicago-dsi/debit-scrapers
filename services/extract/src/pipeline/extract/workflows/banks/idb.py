"""Inter-American Development Bank (IDB)

Data is retrieved through two different strategies and then combined.
In the first, a workflow requests an authentication token from IDB's backend
and then uses that token to fetch an Excel file containing project records.
In the second strategy, a set of workflows are used to crawl IDB's website
and scrape project webpages for data.

This two-pronged approach is necessary because, to the best of the author's
knowledge, some records and fields found in the Excel file are not present on
the website and vice versa, and the website only shows projects with an
approval date rather than the full historical record.
"""

# Standard library imports
import json
import warnings
from datetime import datetime
from io import BytesIO

# Third-party imports
import numpy as np
import pandas as pd
from bs4 import BeautifulSoup
from django.conf import settings

# Application imports
from extract.workflows.abstract import (
    ProjectPartialDownloadWorkflow,
    ProjectPartialScrapeWorkflow,
    ResultsScrapeWorkflow,
    SeedUrlsWorkflow,
)


class IdbPartialProjectDownloadWorkflow(ProjectPartialDownloadWorkflow):
    """Downloads and parses project details from IBD's website."""

    @property
    def download_url(self) -> str:
        """The URL containing all project records."""
        return "https://wabi-us-east2-redirect.analysis.windows.net/export/xlsx"

    @property
    def next_workflow(self) -> str:
        """The name of the workflow to execute, if any. Overrides parent."""
        return settings.SEED_URLS_WORKFLOW

    @property
    def project_page_url(self) -> str:
        """The URL for an IBD project page."""
        return "https://www.iadb.org/en/project/{}"

    @property
    def token_url(self) -> str:
        """The URL used to fetch an authentication token (necessary for downloads)."""
        return "https://www.iadb.org/idb_powerbi_refresh_token/ea1a9698-4380-4bda-9c30-422227708623"

    def get_projects(self) -> pd.DataFrame:
        """Downloads an Excel file of projects from IDB's website.

        Args:
            `None`

        Returns:
            The raw project records.
        """
        # Load download options from file
        try:
            with open(settings.IDB_DOWNLOAD_OPTIONS_FPATH) as f:
                download_options = json.load(f)
        except FileNotFoundError:
            raise RuntimeError(
                "Error fetching IDB download options from file. "
                "The file does not exist."
            ) from None
        except json.JSONDecodeError:
            raise RuntimeError(
                "Error parsing IDB download options into JSON."
            ) from None

        # Fetch authentication token
        r = self._data_request_client.get(self.token_url, use_random_delay=True)

        # Raise error if token not received successfully
        if not r.ok:
            raise RuntimeError(
                "Error fetching authentication token from IBD. The request "
                f'failed with a "{r.status_code} - {r.reason}" status '
                f'code and the message "{r.text}".'
            )

        # Othrwise, parse token
        try:
            token = r.json()["token"]["token"]
        except (KeyError, json.JSONDecodeError):
            raise RuntimeError(
                "Error parsing IDB authentication token from response body."
            ) from None

        # Download project records
        r = self._data_request_client.post(
            self.download_url,
            custom_headers={"Authorization": f"EmbedToken {token}"},
            json=download_options,
        )

        # Raise error if download failed
        if not r.ok:
            raise RuntimeError(
                "Error downloading Excel file from IBD. The request "
                f'failed with a "{r.status_code} - {r.reason}" status '
                f'code and the message "{r.text}".'
            )

        # Load XLSX data into a Pandas DataFrame
        # NOTE: Suppress warnings that the workbook has no default style
        try:
            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                df = pd.read_excel(BytesIO(r.content), engine="openpyxl")
        except Exception as e:
            raise RuntimeError(
                f"Error reading Excel project data from IBD into Pandas DataFrame. {e}"
            ) from None

        return df

    def clean_projects(self, df: pd.DataFrame) -> tuple[list[str], pd.DataFrame]:
        """Cleans project records and parses the next set of URLs to crawl.

        Args:
            df: The raw project records.

        Returns:
            A two-item tuple consisting of the new URLs and cleaned records.
        """
        # Drop last three rows holding summary statistics
        df = df.drop(df.tail(3).index)

        # Replace missing values
        df = df.replace({np.nan: None, "Not Defined": "", "1/1/1901": ""})

        # Add bank column
        df["bank"] = settings.IDB_ABBREVIATION.upper()

        # Add countries column
        def format_countries(val: str | None) -> str:
            """Formats a string of country names.

            Transforms the input into a pipe-delimited string
            with erroneneous country values removed.

            Args:
                val: A string of country names separated
                    by a space, followed by a semicolon.

            Returns:
                A pipe-delimited list of country names.
            """
            if not val:
                return ""

            excluded = ["BANKWIDE", "HEADQUARTERS", "REGIONAL"]
            parts = [p for p in val.split("; ") if p and p.upper() not in excluded]
            return "|".join(parts)

        df["countries"] = df["Project Country"].apply(format_countries)

        # Add project affiliates column
        df["affiliates"] = df.apply(
            lambda row: "|".join(
                elem for elem in [row["Borrower"], row["Executing Agency"]] if elem
            ),
            axis=1,
        )

        # Add URL column
        df["url"] = df["Project Number"].apply(
            lambda proj_num: self.project_page_url.format(proj_num)
        )

        # Finalize column schema
        col_map = {
            "affiliates": "affiliates",
            "countries": "countries",
            "Project Type": "finance_types",
            "Last Updated": "last_updated",
            "Project Name": "name",
            "Project Number": "number",
            "Sector": "sectors",
            "bank": "source",
            "url": "url",
        }
        df = df.rename(columns=col_map)[col_map.values()]

        # Standardize date formats
        def parse_dates(val: str) -> str:
            try:
                return pd.to_datetime(val).strftime("%Y-%m-%d")
            except ValueError:
                return ""

        df["last_updated"] = df["last_updated"].apply(parse_dates)

        # Replace None with empty strings in string data columns
        cols = [
            "affiliates",
            "countries",
            "finance_types",
            "last_updated",
            "name",
            "number",
            "sectors",
        ]
        df[cols] = df[cols].replace({None: ""})

        # Group by project number and perform aggregations
        def concatenate_values(
            group: pd.DataFrame, col_name: str, delimiter: str = "|"
        ) -> str:
            """A generic function for grouping and concatenating values.

            Args:
                group: The group.

                col_name: The column for which to
                    concatenate values.

                delimiter: The string used to join values.
                    Defaults to a pipe ("|").

            Returns:
                The concatenated values.
            """
            lst = []
            for name in group[col_name].tolist():
                lst.extend(name.split("|"))
            return delimiter.join(sorted(set(lst)))

        aggregated_projects = []
        for _, grp in df.groupby("number"):
            first = grp.iloc[0]
            aggregated_projects.append(
                {
                    "affiliates": concatenate_values(grp, "affiliates"),
                    "countries": concatenate_values(grp, "countries"),
                    "finance_types": concatenate_values(grp, "finance_types"),
                    "date_last_updated": grp["last_updated"].max(),
                    "name": first["name"],
                    "number": first["number"],
                    "sectors": concatenate_values(grp, "sectors"),
                    "source": first["source"],
                    "url": first["url"],
                }
            )

        return [""], pd.DataFrame(aggregated_projects)


class IdbSeedUrlsWorkflow(SeedUrlsWorkflow):
    """Retrieves the first set of IDB URLs to scrape."""

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
        return "https://www.iadb.org/en/project-search?page={page_num}"

    def _find_last_page(self) -> int:
        """Retrieves the number of the last search results page.

        Args:
            `None`

        Returns:
            The page number.
        """
        # Fetch page
        params = {"page_num": self.first_page_num}
        first_results_page = self.search_results_base_url.format(**params)
        r = self._data_request_client.get(
            first_results_page,
            use_random_user_agent=True,
            use_random_delay=True,
        )

        # Check response
        if not r.ok:
            raise RuntimeError(
                f"Error fetching search results page "
                f"from IDB. The request failed with a "
                f'"{r.status_code} - {r.reason}" status '
                f'code and the message "{r.text}".'
            )

        # Extract last page number from webpage HTML
        try:
            soup = BeautifulSoup(r.text, "html.parser")
            last_page_btn = soup.find("li", {"class": "pager__item--last"})
            last_page_num = int(
                last_page_btn.find("idb-button")["button-url"].split("=")[-1]
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
                self.search_results_base_url.format(page_num=num)
                for num in range(self.first_page_num, last_page_num + 1)
            ]
            return result_pages
        except Exception as e:
            raise RuntimeError(
                f"Failed to generate IDB search result pages to crawl. {e}"
            ) from None


class IdbResultsScrapeWorkflow(ResultsScrapeWorkflow):
    """Scrapes an IDB search results page for project webpage URLs."""

    @property
    def next_workflow(self) -> str:
        """The next workflow to execute. Overrides parent."""
        return settings.PROJECT_PARTIAL_PAGE_WORKFLOW

    @property
    def project_page_base_url(self) -> str:
        """The base URL for an IBD project page."""
        return "https://www.iadb.org"

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
        )

        # Check response
        if not r.ok:
            raise RuntimeError(
                f"Error fetching search results page "
                f"from IDB. The request failed with a "
                f'"{r.status_code} - {r.reason}" status '
                f'code and the message "{r.text}".'
            )

        # Parse webpage HTML into node tree and scrape table for URLs
        try:
            soup = BeautifulSoup(r.text, "html.parser")
            data_table = soup.find("idb-table")
            urls = [
                f"{self.project_page_base_url}{a['href']}"
                for a in data_table.find_all("a")
                if a["href"].startswith("/en/project/")
            ]
        except Exception as e:
            raise RuntimeError(
                f"Error scraping project page URLs from '{url}'. {e}"
            ) from None

        return urls


class IdbProjectPartialScrapeWorkflow(ProjectPartialScrapeWorkflow):
    """Scrapes an IDB search results page for project data only."""

    def scrape_project_page(self, url: str) -> list[dict]:
        """Extracts an IDB project's name, total funding, status, and approval date.

        Args:
            url: The URL to the project webpage.

        Returns:
            The raw record(s).
        """
        # Request page
        r = self._data_request_client.get(
            url=url,
            use_random_user_agent=True,
            use_random_delay=True,
        )

        # Check response
        if not r.ok:
            raise RuntimeError(
                f"Error fetching project page from "
                f"IDB. The request failed with a "
                f'"{r.status_code} - {r.reason}" status '
                f'code and the message "{r.text}".'
            )

        # Initialize project
        project = {
            "source": settings.IDB_ABBREVIATION.upper(),
            "url": url,
        }

        # Parse webpage HTML into node tree
        soup = BeautifulSoup(r.text, "html.parser")

        # Scrape section wrapper for project name
        try:
            project["name"] = soup.find("idb-section-wrapper", {"eyebrow": "Projects"})[
                "heading"
            ]
        except Exception as e:
            raise RuntimeError(f'Error scraping project name at "{url}". {e}') from None

        # Scrape table for project data
        try:
            for row in soup.find_all("idb-project-table-row"):
                stat_type = row.find("p", {"slot": "stat-type"}).text.strip()
                stat_data = row.find("p", {"slot": "stat-data"}).text.strip()

                if stat_type == "Approval Date":
                    try:
                        parsed_date = datetime.strptime(stat_data, "%B %d, %Y")
                        formatted_date = parsed_date.strftime("%Y-%m-%d")
                        project["date_approved"] = formatted_date
                    except ValueError:
                        project["date_approved"] = ""

                elif stat_type == "Project Status":
                    project["status"] = stat_data

                elif stat_type == "Original Amount Approved":
                    try:
                        currency, amount = stat_data.split(" ")
                        parsed_amount = float(amount.replace(",", ""))
                        project["total_amount_currency"] = currency
                        project["total_amount"] = parsed_amount
                        project["total_amount_usd"] = (
                            parsed_amount if currency == "USD" else None
                        )
                    except ValueError:
                        project["total_amount_currency"] = ""
                        project["total_amount"] = None
                        project["total_amount_usd"] = None

        except Exception as e:
            raise RuntimeError(
                f'Error scraping project table at "{url}". {e}'
            ) from None

        return [project]
