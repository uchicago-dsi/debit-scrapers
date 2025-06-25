"""Data extraction workflows for Norges Bank Investment Management (NBIM).
Data is retrieved by downloading JSON data for the equity, fixed income,
and real estate investment project types for each year available.
"""

# Standard library imports
from datetime import datetime
from urllib.parse import quote

# Third-party imports
import requests
import pandas as pd
from django.conf import settings

# Application imports
from extract.workflows.abstract import ProjectDownloadWorkflow


class NbimDownloadWorkflow(ProjectDownloadWorkflow):
    """Downloads project records directly from NBIM and then cleans
    and saves the data to a database using the `execute` method
    defined in its superclass.
    """

    @property
    def investments_base_url(self) -> str:
        """The base URL for NBIM investments."""
        return "https://www.nbim.no/en/investments/all-investments/#"

    @property
    def download_url(self) -> str:
        """The URL containing all project records."""
        return "https://www.nbim.no/api/investments/history.json?year={}"

    @property
    def project_start_year(self) -> int:
        """The inclusive start year to use when querying NBIM projects."""
        return 1998

    @property
    def project_end_year(self) -> int:
        """The inclusive end year to use when querying NBIM projects."""
        return datetime.now().year

    def get_projects(self) -> pd.DataFrame:
        """Retrieves all development bank projects by downloading
        JSON data from NBIM's website for each year.

        Args:
            `None`

        Returns:
            The raw project records.
        """
        try:
            projects_df = None

            for year in range(self.project_start_year, self.project_end_year + 1):
                # Query API for projects in given year
                projects_url = self.download_url.format(year)
                r = requests.get(projects_url)

                # Check response status
                if not r.ok:
                    raise Exception(
                        f"HTTP GET request for '{projects_url}' failed "
                        f"with status code '{r.status_code}'."
                    )

                # Retrieve JSON from HTTP response body if available
                try:
                    data = r.json()
                except Exception:
                    continue

                # Skip processing for year if no data exists
                if not data:
                    continue

                # Extract data from JSON
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

                # Update projects DataFrame
                equities_df = pd.DataFrame(equities)
                equities_df["type"] = "equities"

                fixed_income_df = pd.DataFrame(fixed_income)
                fixed_income_df["type"] = "fixed-income"

                real_estate_df = pd.DataFrame(real_estate)
                real_estate_df["type"] = "real-estate"

                year_df = pd.concat(
                    [equities_df, fixed_income_df, real_estate_df], sort=True
                )
                year_df["year"] = year

                projects_df = (
                    year_df
                    if projects_df is None
                    else pd.concat([projects_df, year_df])
                )

            return projects_df

        except Exception as e:
            raise Exception(f"Error retrieving NBIM investment data. {e}")

    def clean_projects(self, df: pd.DataFrame) -> pd.DataFrame:
        """Cleans NBIM project records to conform to an expected schema.

        Args:
            df: The raw project records.

        Returns:
            The cleaned records.
        """
        try:
            # Rename existing columns
            df = df.rename(
                columns={"s": "sectors", "ic": "countries", "n": "companies"}
            )

            # Construct project URLs
            def create_project_url(row: pd.Series):
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
                    f"{row['type']}/"
                    f"{int(row['id'])}/"
                    f"{quote(row['companies'])}"
                )

            df["url"] = df.agg(lambda row: create_project_url(row), axis="columns")

            # Define other new columns
            df["bank"] = settings.NBIM_ABBREVIATION.upper()
            df["number"] = df["id"].astype(int)
            df["name"] = df["companies"]
            df["status"] = None
            df["effective_utc"] = df["year"]
            df["amount"] = df["h"].str["v"]
            df["currency"] = "NOK"
            df["amount_usd"] = df["h"].str["vu"]

            # Set final column schema
            col_mapping = {
                "bank": "object",
                "number": "object",
                "name": "object",
                "status": "object",
                "effective_utc": "object",
                "type": "object",
                "amount": "Float64",
                "currency": "object",
                "amount_usd": "Float64",
                "sectors": "object",
                "countries": "object",
                "companies": "object",
                "url": "object",
            }

            # Replace blanks with None
            df = df.replace([""], None, regex=True)

            return df[col_mapping.keys()].astype(col_mapping)

        except Exception as e:
            raise Exception(f"Error cleaning NBIM investment data. {e}")
