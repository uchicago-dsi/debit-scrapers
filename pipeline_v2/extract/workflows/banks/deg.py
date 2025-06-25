"""Data extraction workflows for the German Investment Corporation,
also known as Deutsche Investitions- und Entwicklungsgesellschaft
(DEG), a subsdiary of development bank KFW (Kreditanstalt
für Wiederaufbau). Currently downloads project data as JSON.
"""

# Third-party imports
import numpy as np
import pandas as pd
from django.conf import settings

# Application imports
from extract.workflows.abstract import ProjectDownloadWorkflow


class DegDownloadWorkflow(ProjectDownloadWorkflow):
    """Downloads project records directly from DEG's website and
    then cleans and saves the data to a database using the
    `execute` method defined in its superclass.
    """

    @property
    def download_url(self) -> str:
        """The URL containing all project records."""
        return "https://deginvest-investments.de/?tx_deginvests_rest%5Baction%5D=list&tx_deginvests_rest%5Bcontroller%5D=Rest&cHash=f8602c3bfb7e71d9760e1412bc0c8bb5"

    @property
    def project_detail_base_url(self) -> str:
        """The base URL for individual project pages."""
        return "https://deginvest-investments.de"

    def get_projects(self) -> pd.DataFrame:
        """Retrieves all development bank projects as JSON from
        DEG's website.

        Args:
            None

        Returns:
            The raw project records.
        """
        try:
            response = self._data_request_client.get(self.download_url)
            return pd.DataFrame.from_dict(response.json())
        except Exception as e:
            raise Exception(f"Error retrieving or parsing DEG project JSON. {e}")

    def clean_projects(self, df: pd.DataFrame) -> pd.DataFrame:
        """Cleans DEG project records to conform to an expected schema.

        Args:
            df: The raw project records.

        Returns:
            The cleaned records.
        """
        try:
            # Parse date column
            df["signed_utc"] = df["signingDate"].str[:10]

            # Define additional columns
            df["bank"] = settings.DEG_ABBREVIATION.upper()
            df["number"] = df["uid"]
            df["name"] = df["title"]
            df["status"] = None
            df["loan_amount"] = df["financingSum"]
            df["loan_amount_currency"] = df["currency"].str["code"]
            df["loan_amount_usd"] = df.apply(
                lambda row: row["financingSum"]
                if row["currency"]["code"] == "USD"
                else None,
                axis=1,
            )
            df["sectors"] = df["sector"].str["title"]
            df["countries"] = df["country"].str["title"]
            df["companies"] = df["customer"].str["title"]
            df["url"] = self.project_detail_base_url + df["detailUrl"]

            # Set final column schema
            col_mapping = {
                "bank": "object",
                "number": "object",
                "name": "object",
                "status": "object",
                "signed_utc": "object",
                "loan_amount": "Float64",
                "loan_amount_currency": "object",
                "loan_amount_usd": "Float64",
                "sectors": "object",
                "countries": "object",
                "companies": "object",
                "url": "object",
            }
            df = df[col_mapping.keys()].astype(col_mapping)

            # Replace NaN values with None
            return df.replace({np.nan: None})

        except Exception as e:
            raise RuntimeError(f"Error cleaning DEG projects. {e}")
