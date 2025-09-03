"""Norges Bank Investment Management (NBIM)

Data is retrieved by downloading JSON data for the equity, fixed income,
and real estate investment project types for each year available.
"""

# Standard library imports
from datetime import datetime
from urllib.parse import quote

# Third-party imports
import pandas as pd
from django.conf import settings

# Application imports
from extract.workflows.abstract import (
    ProjectScrapeWorkflow,
    SeedUrlsWorkflow,
)


class NbimSeedUrlsWorkflow(SeedUrlsWorkflow):
    """Generates a list of API endpoints to NBIM project data."""

    @property
    def next_workflow(self) -> str:
        """The next workflow to execute."""
        return settings.PROJECT_PAGE_WORKFLOW

    @property
    def search_results_base_url(self) -> str:
        """The base URL for a project search results API endpoint."""
        return "https://www.nbim.no/api/investments/history.json?year={}"

    @property
    def project_start_year(self) -> int:
        """The inclusive start year to use when querying NBIM projects."""
        return 1998

    @property
    def project_end_year(self) -> int:
        """The inclusive end year to use when querying NBIM projects."""
        return datetime.now().year

    def generate_seed_urls(self) -> list[str]:
        """Generates the first set of API endpoints to query.

        Args:
            `None`

        Returns:
            The unique list of endpoints.
        """
        try:
            return [
                self.search_results_base_url.format(year)
                for year in range(
                    self.project_start_year, self.project_end_year + 1
                )
            ]
        except Exception as e:
            raise RuntimeError(
                f"Failed to generate NBIM project pages to crawl. {e}"
            ) from None


class NbimProjectScrapeWorkflow(ProjectScrapeWorkflow):
    """Queries an NBIM API endpoint for development bank project data."""

    @property
    def investments_base_url(self) -> str:
        """The base URL for NBIM investments."""
        return "https://www.nbim.no/en/investments/all-investments/#"

    def scrape_project_page(self, url: str) -> list[dict]:
        """Extracts and cleans project details from an NBIM API payload.

        Args:
            url: The API url.

        Returns:
            The raw project records.
        """
        # Query API for projects in given year
        r = self._data_request_client.get(
            url, use_random_delay=True, use_random_user_agent=True
        )

        # Check response status
        if not r.ok:
            raise RuntimeError(
                f"Error fetching project page "
                f"from NBIM. The request failed with a "
                f'"{r.status_code} - {r.reason}" status '
                f'code and the message "{r.text}".'
            )

        # Retrieve JSON from HTTP response body if available
        try:
            data = r.json()
        except Exception:
            raise RuntimeError(
                "Error parsing NBIM project data into JSON."
            ) from None

        # Skip processing if no data exists
        if not data:
            return []

        # Otherwise, extract data from JSON
        equities = []
        fixed_income = []
        real_estate = []
        for continent in data["re"]:
            for country in continent["ct"]:
                if "eq" in country.keys():
                    equities.extend(country["eq"]["cp"])
                if "fi" in country.keys():
                    fixed_income.extend(country["fi"]["cp"])
                if "re" in country.keys():
                    real_estate.extend(country["re"]["cp"])

        # Extract investment types into DataFrames
        equities_df = pd.DataFrame(equities)
        equities_df["finance_types"] = "equities"

        fixed_income_df = pd.DataFrame(fixed_income)
        fixed_income_df["finance_types"] = "fixed-income"

        real_estate_df = pd.DataFrame(real_estate)
        real_estate_df["finance_types"] = "real-estate"

        # Concatenate DataFrames
        df = pd.concat(
            [equities_df, fixed_income_df, real_estate_df], sort=True
        )
        df["year"] = int(url.split("year=")[1])

        # Rename existing columns
        df = df.rename(
            columns={"s": "sectors", "ic": "countries", "n": "affiliates"}
        )

        # Construct project URLs
        def create_project_url(row: pd.Series) -> str:
            """Constructs a URL for an NBIM project page.

            Args:
                row: A row of data from the DataFrame.

            Returns:
                The URL.
            """
            return (
                f"{self.investments_base_url}/"
                f"{row['year']}/"
                "investments/"
                f"{row['finance_types']}/"
                f"{int(row['id'])}/"
                f"{quote(row['affiliates'])}"
            )

        df["url"] = df.agg(lambda row: create_project_url(row), axis="columns")

        # Define other new columns
        df["source"] = settings.NBIM_ABBREVIATION.upper()
        df["number"] = df["id"].astype(int)
        df["name"] = df["affiliates"]
        df["status"] = ""
        df["date_effective"] = df["year"]
        df["total_amount"] = df["h"].str["v"]
        df["total_amount_currency"] = "NOK"
        df["total_amount_usd"] = df["h"].str["vu"]

        # Set final column schema
        col_mapping = {
            "affiliates": "object",
            "countries": "object",
            "date_effective": "object",
            "finance_types": "object",
            "name": "object",
            "number": "object",
            "sectors": "object",
            "source": "object",
            "status": "object",
            "total_amount": "Float64",
            "total_amount_currency": "object",
            "total_amount_usd": "Float64",
            "url": "object",
        }
        df = df[col_mapping.keys()].astype(col_mapping)

        # Replace None values with empty strings for string data columns
        cols = [k for k, v in col_mapping.items() if v == "object"]
        df[cols] = df[cols].replace({None: ""})

        return df.to_dict("records")
