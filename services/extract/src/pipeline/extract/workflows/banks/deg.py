"""Deutsche Investitions- und Entwicklungsgesellschaft (DEG)

Data extraction workflows for the German Investment Corporation,
a subsdiary of development bank KFW (Kreditanstalt für Wiederaufbau).
Data is retrieved by downloading separate CSV and JSON files from
DEG's website and then merging the records on common fields.
"""

# Standard library imports
import io
import re
from datetime import datetime

# Third-party imports
import numpy as np
import pandas as pd
from django.conf import settings

# Application imports
from extract.workflows.abstract import ProjectDownloadWorkflow


class DegDownloadWorkflow(ProjectDownloadWorkflow):
    """Downloads and parses files containing DEG project data."""

    @property
    def csv_download_url(self) -> str:
        """The URL to a CSV file with project records."""
        return "https://deginvest-investments.de/deginvest.csv"

    @property
    def json_download_url(self) -> str:
        """The URL to a JSON file with project records."""
        return "https://deginvest-investments.de/?tx_deginvests_rest%5Baction%5D=list&tx_deginvests_rest%5Bcontroller%5D=Rest&cHash=f8602c3bfb7e71d9760e1412bc0c8bb5"

    @property
    def project_detail_base_url(self) -> str:
        """The base URL for individual project pages."""
        return "https://deginvest-investments.de"

    def _get_csv_projects(self) -> pd.DataFrame:
        """Prepares a CSV file of development projects for merge.

        Args:
            `None`

        Returns:
            The raw project records.
        """
        # Fetch CSV project data
        r = self._data_request_client.get(
            self.csv_download_url,
            use_random_delay=True,
            use_random_user_agent=True,
        )
        if not r.ok:
            raise RuntimeError(
                "Error fetching DEG project CSV file. "
                f"The request failed with a "
                f'"{r.status_code} - {r.reason}" status '
                f'code and the message "{r.text}".'
            )

        # Read data into Pandas DataFrame
        try:
            df = pd.read_csv(io.BytesIO(r.content), sep=";", encoding="utf-8")
        except Exception as e:
            raise RuntimeError(
                f"Error parsing DEG project CSV file into DataFrame. {e}"
            ) from None

        # Finalize columns
        try:
            df = df[
                [
                    "Customer Name",
                    "Country",
                    "Signing Date",
                    "Funding",
                    "Investment Instrument",
                ]
            ]
            df.loc[:, "Customer Name"] = df["Customer Name"].apply(
                lambda s: re.sub(r"\s+", " ", s)
            )
        except Exception as e:
            raise RuntimeError(
                f"Error finalizing columns for DEG CSV data. {e}"
            ) from None

        return df

    def _get_json_projects(self) -> pd.DataFrame:
        """Prepares a JSON file of development projects for merge.

        Args:
            `None`

        Returns:
            The raw project records.
        """
        # Fetch JSON project data
        r = self._data_request_client.get(
            self.json_download_url,
            use_random_delay=True,
            use_random_user_agent=True,
        )
        if not r.ok:
            raise RuntimeError(
                "Error fetching DEG project JSON file. "
                f"The request failed with a "
                f'"{r.status_code} - {r.reason}" status '
                f'code and the message "{r.text}".'
            )

        # Read data into Pandas DataFrame
        try:
            df = pd.DataFrame.from_dict(r.json())
        except Exception as e:
            raise RuntimeError(
                f"Error parsing DEG project JSON file into DataFrame. {e}"
            ) from None

        # Prepare columns for merge
        try:
            df["Customer Name"] = df["title"].str.replace(r"\s+", " ")
            df["Country"] = df["country"].str["title"]
            df["Signing Date"] = df["signingDate"].apply(
                lambda dt: datetime.fromisoformat(dt).strftime("%m-%Y")
            )
            df["Funding"] = (
                df["currency"].str["code"] + " " + df["financingSum"].astype(str)
            )
        except Exception as e:
            raise RuntimeError(
                f"Error finalizing columns for DEG CSV data. {e}"
            ) from None

        return df

    def get_projects(self) -> pd.DataFrame:
        """Downloads all development bank projects from DEG's website.

        Args:
            `None`

        Returns:
            The raw project records.
        """
        try:
            # Retrieve available project datasets for merge
            df1 = self._get_csv_projects()
            df2 = self._get_json_projects()

            # Merge datasets on common fields
            merged_df = df2.merge(
                df1,
                how="left",
                on=["Customer Name", "Country", "Signing Date", "Funding"],
            )

            # Rename investment column
            merged_df = merged_df.rename(
                columns={"Investment Instrument": "investmentInstrument"}
            )

            # Drop unused columns
            merged_df = merged_df.drop(
                columns=[
                    "Customer Name",
                    "Country",
                    "Signing Date",
                    "Funding",
                ]
            )

            return merged_df

        except Exception as e:
            raise RuntimeError(f"Failed to download DEG project data. {e}") from None

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
            df["finance_types"] = df["investmentInstrument"]
            df["total_amount"] = df["financingSum"]
            df["total_amount_currency"] = df["currency"].str["code"]
            df["total_amount_usd"] = df.apply(
                lambda row: (
                    row["financingSum"] if row["currency"]["code"] == "USD" else None
                ),
                axis=1,
            )
            df["sectors"] = df["sector"].str["title"]
            df["countries"] = df["country"].str["title"]
            df["affiliates"] = df["title"]
            df["url"] = self.project_detail_base_url + df["detailUrl"]

            # Set final column schema
            col_mapping = {
                "affiliates": "object",
                "countries": "object",
                "date_signed": "object",
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

            # Replace NaN values with None
            df = df.replace({np.nan: None})

            # Replace None values with empty strings for string columns
            cols = [k for k, v in col_mapping.items() if v == "object"]
            df[cols] = df[cols].replace({None: ""})

            return df

        except Exception as e:
            raise RuntimeError(f"Error cleaning DEG projects. {e}") from None
