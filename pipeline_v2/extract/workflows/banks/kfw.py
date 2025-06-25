"""Data extraction workflows for the development bank KFW (Kreditanstalt
für Wiederaufbau). Data currently retrieved by downloading project JSON.
"""

# Standard library imports
from datetime import datetime

# Third-party imports
import numpy as np
import pandas as pd
from django.conf import settings

# Application imports
from extract.workflows.abstract import ProjectDownloadWorkflow


class KfwDownloadWorkflow(ProjectDownloadWorkflow):
    """Downloads project records directly from KFW's website
    and then cleans and saves the data to a database.
    """

    @property
    def download_url(self) -> str:
        """The URL containing all project records."""
        return "https://www.kfw-entwicklungsbank.de/ipfz/Projektdatenbank/download/json"

    @property
    def projects_base_url(self) -> str:
        """The base URL for individual KFW project pages."""
        return "https://www.kfw-entwicklungsbank.de/ipfz/Projektdatenbank"

    def get_projects(self) -> pd.DataFrame:
        """Retrieves all development bank projects as JSON from KFW's website.

        Args:
            `None`

        Returns:
            The raw project records.
        """
        try:
            return pd.read_json(self.download_url)
        except Exception as e:
            raise Exception(f"Error retrieving JSON project data from KFW. {e}")

    def clean_projects(self, df: pd.DataFrame) -> pd.DataFrame:
        """Cleans KFW project records to conform to an expected schema.

        Args:
            df: The raw project records.

        Returns:
            The cleaned records.
        """
        try:
            # Replace missing values with None
            df = df.replace({np.nan: None})

            # Set parent bank name
            df["bank"] = settings.KFW_ABBREVIATION.upper()

            # Set project number
            df["number"] = df["projnr"]

            # Set project title
            df["name"] = df["title"]

            # Set date of last project update
            df["last_updated_utc"] = df["hostDate"].apply(
                lambda date: datetime.strptime(date, "%B %d, %Y").strftime("%Y-%m-%d")
                if date
                else None
            )

            # Set project finance type
            df["type"] = df["finanzierungsinstrument"]

            # Set project finance amount and currency
            df["amount"] = df["amount"] * 10**6
            df["currency"] = "EUR"
            df["amount_usd"] = None

            # Set project sectors
            df["sectors"] = df["crscode2"]

            # Set project countries
            df["countries"] = df["country"]

            # Set project companies
            def get_companies(row: pd.Series) -> str:
                """Returns a pipe-delimited list of project companies.

                Args:
                    row: A row of data from the DataFrame.

                Returns:
                    The list of companies.
                """
                principal_map = {
                    "BMZ": "German Federal Ministry for Economic Coperation and Development (BMZ)",
                    "BMWK": "German Federal Ministry for Economic Affairs and Energy (BMWK)",
                    "AA": "German Federal Foreign Office (AA)",
                    "BMUV": "German Federal Ministry for the Environment, Nature Conservation, Nuclear Safety and Consumer Protection (BMUV)",
                    "BMF": "German Federal Ministry of Finance (BMF)",
                }
                return "|".join(
                    [
                        *row["projekttraegers"],
                        *row["kofinanzpartners"],
                        principal_map[row["principal"]],
                    ]
                )

            df["companies"] = df.apply(get_companies, axis=1)

            # Set project URL
            def create_project_url(row: pd.Series):
                """Constructs a URL for a KFW project page.

                Args:
                    row: A row of data from the DataFrame.

                Returns:
                    The URL.
                """
                return (
                    f"{self.projects_base_url}/"
                    f"{row['name'].replace(' ', '-')}-"
                    f"{row['number']}.htm"
                )

            df["url"] = df.agg(lambda row: create_project_url(row), axis="columns")

            # Set final column schema
            col_mapping = {
                "bank": "object",
                "number": "object",
                "name": "object",
                "status": "object",
                "type": "object",
                "last_updated_utc": "object",
                "amount": "Float64",
                "currency": "object",
                "amount_usd": "Float64",
                "sectors": "object",
                "countries": "object",
                "companies": "object",
                "url": "object",
            }

            return df[col_mapping.keys()].astype(col_mapping)

        except Exception as e:
            raise Exception(f"Error cleaning KFW project data. {e}")
