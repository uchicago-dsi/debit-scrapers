"""Inter-American Development Bank (IDB)

Data is retrieved by requesting an authentication token from IDB's backend
and then using that token to request an Excel file containing project records.
"""

# Standard library imports
import json
import warnings
from io import BytesIO

# Third-party imports
import numpy as np
import pandas as pd
from django.conf import settings

# Application imports
from extract.workflows.abstract import ProjectDownloadWorkflow


class IdbProjectDownloadWorkflow(ProjectDownloadWorkflow):
    """Downloads project details from IBD's website."""

    @property
    def download_url(self) -> str:
        """The URL containing all project records."""
        return "https://wabi-us-east2-redirect.analysis.windows.net/export/xlsx"

    @property
    def project_page_url(self) -> str:
        """The base URL for an IBD project page."""
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
        r = self._data_request_client.get(self.token_url, use_random_user_agent=True)

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

    def clean_projects(self, df: pd.DataFrame) -> pd.DataFrame:
        """Cleans project records to conform to an expected schema.

        Args:
            df: The raw project records.

        Returns:
            The cleaned records.
        """
        # Drop last three rows holding summary statistics
        df = df.drop(df.tail(3).index)

        # Replace NaNs with None
        df = df.replace({np.nan: None, "Not Defined": None, "1901-01-01": None})

        # Add bank column
        df["bank"] = "IDB"

        # Add loan amount in USD column
        df["total_amount_in_usd"] = df.apply(
            lambda row: (row["Approval Amount"] if row["Currency"] == "USD" else None),
            axis=1,
        )

        # Add countries column
        def format_countries(val: str | None) -> str | None:
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
                return None

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

        # Finalize columns
        col_map = {
            "affiliates": "affiliates",
            "countries": "countries",
            "Approval Date": "date_approved",
            "Signature Date": "date_signed",
            "Project Name": "name",
            "Project Number": "number",
            "Sector": "sectors",
            "bank": "source",
            "Status": "status",
            "Approval Amount": "total_amount",
            "Currency": "total_amount_currency",
            "total_amount_in_usd": "total_amount_usd",
            "url": "url",
        }
        df = df.rename(columns=col_map)[col_map.values()]

        return df
