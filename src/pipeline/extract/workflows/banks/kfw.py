"""Kreditanstalt für Wiederaufbau (KFW)

Data is retrieved by downloading project JSON.
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
    """Downloads and parses a JSON file containing project data."""

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
        # Fetch project data
        r = self._data_request_client.get(url=self.download_url)
        if not r.ok:
            raise RuntimeError(
                "Error fetching KFW project records. "
                f"The request failed with a "
                f'"{r.status_code} - {r.reason}" status '
                f'code and the message "{r.text}".'
            )

        # Parse projects into JSON
        try:
            df = pd.DataFrame.from_dict(r.json())
        except Exception as e:
            raise RuntimeError(
                f"Error parsing KFW projects into DataFrame. {e}"
            ) from None

        return df

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
            df["source"] = settings.KFW_ABBREVIATION.upper()

            # Set project number
            df["number"] = df["projnr"]

            # Set project title
            df["name"] = df["title"]

            # Set date of last project update
            df["date_last_updated"] = df["hostDate"].apply(
                lambda date: (
                    datetime.strptime(date, "%b %d, %Y").strftime("%Y-%m-%d")
                    if date
                    else ""
                )
            )

            # Set project finance type
            df["finance_types"] = df["finanzierungsinstrument"]

            # Set project finance amount and currency
            df["total_amount"] = df["amount"] * 10**6
            df["total_amount_currency"] = "EUR"

            # Set project sectors
            df["sectors"] = df["crscode2"]

            # Set project countries
            df["countries"] = df["country"]

            # Set project affiliates
            def get_affiliates(row: pd.Series) -> str:
                """Returns a pipe-delimited list of project affiliates.

                Args:
                    row: A row of data from the DataFrame.

                Returns:
                    The list of affiliates.
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

            df["affiliates"] = df.apply(get_affiliates, axis=1)

            # Set project URL
            def create_project_url(row: pd.Series) -> str:
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

            df["url"] = df.agg(
                lambda row: create_project_url(row), axis="columns"
            )

            # Set final column schema
            col_mapping = {
                "affiliates": "object",
                "countries": "object",
                "date_last_updated": "object",
                "finance_types": "object",
                "name": "object",
                "number": "object",
                "sectors": "object",
                "source": "object",
                "status": "object",
                "total_amount": "Float64",
                "total_amount_currency": "object",
                "url": "object",
            }

            df = df[col_mapping.keys()].astype(col_mapping)

            # Replace None with empty strings for string data columns
            cols = [k for k, v in col_mapping.items() if v == "object"]
            df[cols] = df[cols].replace({None: ""})

            return df

        except Exception as e:
            raise RuntimeError(
                f"Error cleaning KFW project data. {e}"
            ) from None
