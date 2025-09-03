"""Deutsche Investitions- und Entwicklungsgesellschaft (DEG)

Data extraction workflows for the German Investment Corporation,
a subsdiary of development bank KFW (Kreditanstalt für Wiederaufbau).
Data is retrieved through a direct download.
"""

# Third-party imports
import numpy as np
import pandas as pd
from django.conf import settings

# Application imports
from extract.workflows.abstract import ProjectDownloadWorkflow


class DegDownloadWorkflow(ProjectDownloadWorkflow):
    """Downloads and parses a JSON file containing project data."""

    @property
    def download_url(self) -> str:
        """The URL containing all project records."""
        return "https://deginvest-investments.de/?tx_deginvests_rest%5Baction%5D=list&tx_deginvests_rest%5Bcontroller%5D=Rest&cHash=f8602c3bfb7e71d9760e1412bc0c8bb5"

    @property
    def project_detail_base_url(self) -> str:
        """The base URL for individual project pages."""
        return "https://deginvest-investments.de"

    def get_projects(self) -> pd.DataFrame:
        """Downloads all development bank projects as JSON from DEG's website.

        Args:
            `None`

        Returns:
            The raw project records.
        """
        # Fetch project data
        r = self._data_request_client.get(
            self.download_url,
            use_random_delay=True,
            use_random_user_agent=True,
        )
        if not r.ok:
            raise RuntimeError(
                "Error fetching DEG project data. "
                f"The request failed with a "
                f'"{r.status_code} - {r.reason}" status '
                f'code and the message "{r.text}".'
            )

        # Parse JSON response
        try:
            df = pd.DataFrame.from_dict(r.json())
        except Exception as e:
            raise RuntimeError(
                f"Error parsing DEG projects into DataFrame. {e}"
            ) from None

        return df

    def clean_projects(self, df: pd.DataFrame) -> pd.DataFrame:
        """Cleans DEG project records to conform to an expected schema.

        Args:
            df: The raw project records.

        Returns:
            The cleaned records.
        """
        try:
            # Parse date column
            df["date_signed"] = df["signingDate"].str[:10]

            # Define additional columns
            df["source"] = settings.DEG_ABBREVIATION.upper()
            df["number"] = df["uid"]
            df["name"] = df["title"]
            df["status"] = ""
            df["total_amount"] = df["financingSum"]
            df["total_amount_currency"] = df["currency"].str["code"]
            df["total_amount_usd"] = df.apply(
                lambda row: (
                    row["financingSum"]
                    if row["currency"]["code"] == "USD"
                    else None
                ),
                axis=1,
            )
            df["sectors"] = df["sector"].str["title"]
            df["countries"] = df["country"].str["title"]
            df["affiliates"] = df["customer"].str["title"]
            df["url"] = self.project_detail_base_url + df["detailUrl"]

            # Set final column schema
            col_mapping = {
                "affiliates": "object",
                "countries": "object",
                "date_signed": "object",
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

            # Replace NaN values with None
            df = df.replace({np.nan: None})

            # Replace None values with empty strings for string columns
            cols = [k for k, v in col_mapping.items() if v == "object"]
            df[cols] = df[cols].replace({None: ""})

            return df

        except Exception as e:
            raise RuntimeError(f"Error cleaning DEG projects. {e}") from None
